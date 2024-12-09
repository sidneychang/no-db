package config

import "os"

// type Options string;
type Options struct {
	Nodes       int
	DataPath    string
	SegmentSize int
	// specifies the path to the directory where the database stores its data.
	DirPath string
	// define the maximum of each data file
	DataFileSize int64
	// SyncWrite determines whether the database should ensure data persistence with
	// every write operation.
	SyncWrite bool
}

func NewOptions(nodes int, segmentSize int, DirPath string) *Options {
	return &Options{
		Nodes:        nodes,
		DirPath:      DirPath,
		SegmentSize:  segmentSize,
		DataFileSize: 1024 * 1024 * 256,
		SyncWrite:    false,
	}
}

// IteratorOptions is the configuration for index iteration.
type IteratorOptions struct {
	// Prefix specifies the prefix value for keys to iterate over. Default is empty.
	Prefix []byte

	// Reverse indicates whether to iterate in reverse order.
	// Default is false for forward iteration.
	Reverse bool
}

var DefaultOptions = Options{
	Nodes:        1,
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrite:    false,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
