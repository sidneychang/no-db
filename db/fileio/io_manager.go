package fileio

import "errors"

// file owner can read and write the file
// but others can only read the file
const DataFilePerm = 0644

const DefaultFileSize = 1024 * 1024 * 256

const (
	FileIOType = iota + 1
	BufIOType
	MmapIOType
)

// IOManager is an abstract IO management interface that can accommodate different IO types.
// Currently, it supports standard file IO.
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Close() error
	Sync() error
	Size() (int64, error)
}

func NewIOManager(filePath string, fileSize int64, fioType int8) (IOManager, error) {
	switch fioType {
	case FileIOType:
		return NewFileIOManager(filePath)
	case BufIOType:
		return NewBufIOManager(filePath)
	case MmapIOType:
		return NewMmapIOManager(filePath, fileSize)
	default:
		return nil, errors.New("invalid fioType")
	}
}
