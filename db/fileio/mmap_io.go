package fileio

import (
	"errors"
	"os"

	"github.com/edsrzf/mmap-go"
)

type MMapIO struct {
	fd       *os.File  // system file descriptor
	data     mmap.MMap // the mapping area corresponding to the file
	dirty    bool      // has changed
	offset   int64     // next write location
	fileSize int64     // max file size
}

func NewMmapIOManager(path string, fileSize int64) (*MMapIO, error) {
	mmapIO := &MMapIO{fileSize: fileSize}

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if err := fd.Truncate(fileSize); err != nil {
		return nil, err
	}
	//building mappings between memory and disk files
	b, err := mmap.Map(fd, mmap.RDWR, 0)

	if err != nil {
		return nil, err
	}
	mmapIO.fd = fd
	mmapIO.data = b
	mmapIO.offset = stat.Size()
	return mmapIO, nil

}

func (m *MMapIO) Write(b []byte) (int, error) {
	oldOffset := m.offset
	newOffset := m.offset + int64(len(b))
	if newOffset > m.fileSize {
		return 0, errors.New("exceed file max content length")
	}

	m.offset = newOffset
	m.dirty = true
	return copy(m.data[oldOffset:], b), nil
}

func (m *MMapIO) Read(b []byte, offset int64) (int, error) {
	return copy(b, m.data[offset:]), nil
}

func (m *MMapIO) Sync() error {
	if !m.dirty {
		return nil
	}

	if err := m.data.Flush(); err != nil {
		return err
	}

	m.dirty = false
	return nil
}

func (m *MMapIO) Close() error {
	if err := m.fd.Truncate(m.offset); err != nil {
		return err
	}
	if err := m.Sync(); err != nil {
		return err
	}
	if err := m.UnMap(); err != nil {
		panic(err)
	}
	return m.fd.Close()
}

func (m *MMapIO) Size() (int64, error) {
	return m.offset, nil
}
func (m *MMapIO) UnMap() error {
	if m.data == nil {
		return nil
	}
	err := m.data.Unmap()
	m.data = nil
	return err
}
