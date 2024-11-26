package engine

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/sidneychang/no-db/db/data"
)

var (
	mergeDirName = "dbmerge"
	mergeFinaKey = "mergeFina.finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	if db.isMerging {
		db.lock.Unlock()
		return errors.New("db is merging")
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	if err := db.activeFile.Sync(); err != nil {
		return err
	}

	//convert the current active file to the old data file
	db.olderFiles[db.activeFile.FileID] = db.activeFile
	//open a new active file
	if err := db.setActiveDataFile(); err != nil {
		db.lock.Unlock()
		return nil
	}

	//records files that have not participated in the merge recently
	noMergeFileId := db.activeFile.FileID

	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.lock.Unlock()

	//sort marge files from smallest toe largest
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()

	//if the directory exists, it has been merged and need to be deleted
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//create a merge directory
	if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrite = false

	mergeDB, err := NewDB(mergeOptions)
	if err != nil {
		return err
	}

	//open the hint file storage index
	hintFile, err := data.OpenHintFile(mergePath, db.options.DataFileSize, 1)
	if err != nil {
		return err
	}
	//walk through each data file
	for _, files := range mergeFiles {
		//open the data file
		var offset int64 = 0
		for {
			record, size, err := files.ReadRecord(offset)
			//check if there was an error while reading the record
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := parseRecordKeyAndSeq(record.Key)
			recordPst, err := mergeDB.appendRecord(record)
			if recordPst != nil && recordPst.Fid == files.FileID && recordPst.Offset == offset {
				//parse the key
				record.Key = encodeRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				recordPst, err := mergeDB.appendRecord(record)
				if err != nil {
					return err
				}

				// Writes the current location index to the hint file
				if err := hintFile.WriteHintRecord(realKey, recordPst); err != nil {
					return err
				}
			}
			// Incremental offest
			offset += size

		}
	}

	// persistence
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// Write a file that identifies the merge completion
	mergeFinaFile, err := data.OpenMergeFinaFile(mergePath, db.options.DataFileSize, 1)
	if err != nil {
		return err
	}

	mergeFinaRecord := &data.Record{
		Key:   []byte(mergeFinaKey),
		Value: []byte(strconv.Itoa(int(noMergeFileId))),
	}

	encRecord, _ := data.EncodeRecord(mergeFinaRecord)
	if err := mergeFinaFile.Write(encRecord); err != nil {
		return err
	}

	// persistence
	if err := mergeFinaFile.Sync(); err != nil {
		return err
	}

	return nil
}
func (db *DB) getMergePath() string {
	// Gets the database parent directory
	parentDir := path.Dir(path.Clean(db.options.DirPath))
	// DB base path
	basePath := path.Base(db.options.DirPath)
	// Return the merge file path
	return filepath.Join(parentDir, basePath+mergeDirName)
}

// Load the merge data directory
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// Return the merge directory if it does not exist
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirs, err := os.ReadDir(mergePath)

	// Check if there was an error while reading the directory
	if err != nil {
		return err
	}

	// Find the file that identifies the merge and determine whether the merge is complete
	var mergeFinished bool
	var mergeFileNames []string

	// Iterate over the directories
	for _, dir := range dirs {
		// Check if the directory name matches the merge finish file suffix
		if dir.Name() == data.MergeFinaFileSuffix {
			mergeFinished = true
		}

		// Append the directory name to the mergeFileNames slice
		mergeFileNames = append(mergeFileNames, dir.Name())
	}

	// If not, return directly
	if !mergeFinished {
		return nil
	}

	nonMergeFileID, err := db.getRecentlyNonMergeFileId(mergePath)

	// Check if there was an error while retrieving the recently non-merge file ID
	if err != nil {
		return err
	}

	// Delete old data files
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileID)

		// Check if the file exists
		if _, err := os.Stat(fileName); err == nil {
			// Remove the file
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// Move the new data file to the data directory
	for _, fileName := range mergeFileNames {
		mergeSrcPath := filepath.Join(mergePath, fileName)
		dataSrcPath := filepath.Join(db.options.DirPath, fileName)

		// Rename the file from mergeSrcPath to dataSrcPath
		if err := os.Rename(mergeSrcPath, dataSrcPath); err != nil {
			return err
		}
	}

	return nil

}

// Gets the id of the file that did not participate in the merge recently
func (db *DB) getRecentlyNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinaFile, err := data.OpenMergeFinaFile(dirPath, db.options.DataFileSize, 1)
	if err != nil {
		return 0, err
	}

	// Read the log record at offset 0 from mergeFinaFile
	record, _, err := mergeFinaFile.ReadRecord(0)
	if err != nil {
		return 0, err
	}

	// Convert the value of the log record to an integer
	nonMergeFileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileID), nil

}

// Load the index from the hint file
func (db *DB) loadIndexFromHintFile() error {
	// Check whether the hint file exists
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileSuffix)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// Open hint file
	hintFile, err := data.OpenHintFile(db.options.DirPath, db.options.DataFileSize, 1)
	if err != nil {
		return err
	}

	// Read the index in the file
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Decode to get the actual index location
		pst := data.DecodeRecordPst(logRecord.Value)
		db.index.Put(logRecord.Key, pst)
		offset += size
	}
	return nil
}
