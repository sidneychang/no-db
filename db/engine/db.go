package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/data"
	"github.com/sidneychang/no-db/db/index"
	"go.uber.org/zap"
)

type DB struct {
	options    config.Options
	lock       *sync.RWMutex
	fileIds    []int
	activeFile *data.DataFile
	index      index.Indexer
	olderFiles map[uint32]*data.DataFile
	isMerging  bool
}

const nonTransactionSeqNo = 1

func NewDB(options config.Options) (*DB, error) {
	zap.L().Info("open db", zap.Any("options", options))
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		return nil, err
	}
	db := &DB{
		options:    options,
		olderFiles: make(map[uint32]*data.DataFile),
		lock:       new(sync.RWMutex),
		index:      index.NewIndexer(options.DirPath),
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func checkOptions(options config.Options) error {
	if options.DirPath == "" {
		return errors.New("dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("data file size is invalid")
	}
	return nil
}

func (db *DB) Close() error {
	zap.L().Info("closing db", zap.Any("options", db.options))
	if db.activeFile != nil {
		return nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()

	//close active file
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// close older files
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil

}

// sync the db instance
func (db *DB) Sync() error {
	zap.L().Info("syncing db", zap.Any("options", db.options))
	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()

	//sync active file
	return db.activeFile.Sync()
}

func (db *DB) Put(key []byte, value []byte) error {
	zap.L().Info("putting key", zap.Any("key", string(key)), zap.Any("value", string(value)))
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	record := &data.Record{
		Key:   encodeRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type:  data.Normal,
		Value: value,
	}

	pos, err := db.appendRecordWithLock(record)
	if err != nil {
		return err
	}

	if ok := db.index.Put(key, pos); !ok {
		return errors.New("put key failed")
	}
	return nil
}
func (db *DB) appendRecordWithLock(record *data.Record) (*data.RecordPst, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendRecord(record)
}

func (db *DB) appendRecord(record *data.Record) (*data.RecordPst, error) {
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//write data coding
	encRecord, size := data.EncodeRecord(record)
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// persisting data files to ensure that existing data  on disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//converts the current active file to the old datafile
		db.olderFiles[db.activeFile.FileID] = db.activeFile
		//opens a new active file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}

	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//determin whether to init based on user configuration
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//build in-memory index info
	pst := &data.RecordPst{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
	}
	return pst, nil
}

// set the current active file
// hold a mutex before accessing this method
func (db *DB) setActiveDataFile() error {
	var initFileID uint32
	if db.activeFile != nil {
		initFileID = db.activeFile.FileID + 1
	}
	//open the file
	dataFile, err := data.OpenDataFile(db.options.DirPath, initFileID, db.options.DataFileSize, 1)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil

}
func (db *DB) Get(key []byte) ([]byte, error) {
	zap.L().Info("getting key", zap.Any("key", key))
	db.lock.RLock()
	defer db.lock.RUnlock()

	if len(key) == 0 {
		fmt.Println("key is empyty")
		return nil, errors.New("key is empty")
	}

	recordPst := db.index.Get(key)
	if recordPst == nil {
		fmt.Println("key not found")
		return nil, errors.New("key not found")
	}
	return db.getValueByPosition(recordPst)
}
func (db *DB) GetListKeys() [][]byte {
	iterator := db.index.Iterator(false)

	keys := make([][]byte, db.index.Size())

	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys

}

// Fold get all the data and perform the operation specified by the user.
// The function returns false to exit
func (db *DB) Fold(f func(key []byte, value []byte) bool) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	iterator := db.index.Iterator(false)

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !f(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition Get the corresponding value based on the location index information
func (db *DB) getValueByPosition(recordPst *data.RecordPst) ([]byte, error) {
	var dataFile *data.DataFile
	if recordPst.Fid == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[recordPst.Fid]
	}
	if dataFile == nil {
		return nil, errors.New("data file not found")
	}
	record, _, err := dataFile.ReadRecord(recordPst.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.Deleted {
		return nil, errors.New("record is deleted")
	}
	return record.Value, nil

}

func (db *DB) Delete(key []byte) error {
	zap.L().Info("deleting key", zap.Any("key", key))
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	if pst := db.index.Get(key); pst == nil {
		return nil
	}
	record := &data.Record{
		Key:  encodeRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.Deleted,
	}
	_, err := db.appendRecordWithLock(record)
	if err != nil {
		return err
	}
	ok := db.index.Delete(key)
	if !ok {
		return errors.New("delete key failed")
	}
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntry, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	for _, entry := range dirEntry {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return errors.New("invalid data file name")
			}
			fileIds = append(fileIds, fileId)
		}
	}
	sort.Ints(fileIds)
	db.fileIds = fileIds

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), db.options.DataFileSize, 1)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			//the last id is the larget, the current file is a activefile
			db.activeFile = dataFile
		} else {
			//this file is an old data file
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil

}

func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}
	var hasMerge bool = false
	var nonMergeFileId uint32 = 0
	mergeFileName := filepath.Join(db.options.DirPath, data.MergeFinaFileSuffix)
	//if a file exists, retrieve the id of the file that did not participate in the merge
	if _, err := os.Stat(mergeFileName); err == nil {
		//check if the merge file exist
		// if it exists, determine the id of the most recently non-merge file
		fileId, err := db.getRecentlyNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		nonMergeFileId = fileId
		hasMerge = true
	}
	updateIndex := func(key []byte, typ data.RecordType, pst *data.RecordPst) {
		var ok bool
		if typ == data.Deleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pst)
		}
		if !ok {
			panic("update index failed")
		}
	}
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// If the id is smaller than that of the file that did not participate in the merge recently,
		// the hint file has been loaded
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			recordPst := &data.RecordPst{
				Fid:    fileId,
				Offset: offset,
			}
			realKey, _ := parseRecordKeyAndSeq(record.Key)

			updateIndex(realKey, record.Type, recordPst)

			offset += size
		}
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}

	}
	return nil
}

// // Backup the database to the specified directory
// func (db *DB) Backup(dir string) error {
// 	db.lock.RLock()
// 	defer db.lock.RUnlock()

// 	// Create a backup directory
// 	return backup.CopyDir(db.options.DirPath, dir)
// }

// Clean the DB data directory after the test is complete
func (db *DB) Clean() {
	if db != nil {
		_ = db.Close()
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			_ = fmt.Errorf("clean db error: %v", err)
		}
	}
}

// encodeRecordKeyWithSeq Key+Seq Number coding
func encodeRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	// Create a byte slice to hold the encoded key
	encodeKey := make([]byte, n+len(key))

	// Copy the sequence number bytes to the encodeKey slice
	copy(encodeKey[:n], seq[:n])

	// Copy the original key bytes to the encodeKey slice starting from offset n
	copy(encodeKey[n:], key)

	return encodeKey
}

// Parse the Record key to get the actual key and transaction sequence number seq
func parseRecordKeyAndSeq(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)

	// Extract the real key from the remaining bytes
	realKey := key[n:]

	return realKey, seqNo
}
