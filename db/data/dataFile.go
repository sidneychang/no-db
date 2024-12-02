package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/sidneychang/no-db/db/fileio"
)

const (
	DataFileSuffix      = ".data"
	HintFileSuffix      = "hintIndex"
	MergeFinaFileSuffix = "mergeFina"
)

// DataFile represents a data file.
type DataFile struct {
	FileID    uint32           // File ID
	WriteOff  int64            // Position where the file is currently being written
	IoManager fileio.IOManager // IO read/write operations
}

func OpenDataFile(dirPath string, fileID uint32, fileSize int64, fioType int8) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileID)
	return newDataFile(fileName, fileID, fileSize, fioType)
}

func GetDataFileName(dirPath string, fileID uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+DataFileSuffix)
}
func OpenHintFile(dirPath string, fileSize int64, fioType int8) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileSuffix)
	return newDataFile(fileName, 0, fileSize, fioType)
}
func OpenMergeFinaFile(dirPath string, fileSize int64, fioType int8) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinaFileSuffix)
	return newDataFile(fileName, 0, fileSize, fioType)
}

// WriteHintRecord writes index information to the hint file.
func (df *DataFile) WriteHintRecord(key []byte, pst *RecordPst) error {
	record := &Record{
		Key:   key,
		Value: EncodeRecordPst(pst),
	}
	encRecord, _ := EncodeRecord(record)
	return df.Write(encRecord)
}

func newDataFile(dirPath string, fileId uint32, fileSize int64, fioType int8) (*DataFile, error) {
	ioManager, err := fileio.NewIOManager(dirPath, fileSize, fioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}
func (df *DataFile) ReadRecord(offset int64) (*Record, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxRecordHeaderSize
	if offset == fileSize {
		return nil, 0, io.EOF
	}
	for offset+headerBytes > fileSize {
		//header is not as large as max record header size
		headerBytes = fileSize - offset
	}

	// Read header infomation
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeRecordHeader(headerBuf)
	if header == nil {

		return nil, 0, fmt.Errorf("invalid record header")
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {

		return nil, 0, fmt.Errorf("invalid record header")
	}

	//retrieve the length of the key and value
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	record := &Record{Type: header.recordType}

	if (keySize > 0) || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//Decode
		record.Key = kvBuf[:keySize]
		record.Value = kvBuf[keySize:]
	}
	//verify crc
	crc := calRecordCRC(record, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, fmt.Errorf("invalid record crc")
	}
	return record, recordSize, nil
}
func (df *DataFile) Write(buf []byte) error {
	size, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(size)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IoManager.Read(buf, offset)
	return buf, err
}
