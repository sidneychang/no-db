
package data
import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeRecord(t *testing.T) {
	// Normal condition
	record1 := &Record{
		Key:   []byte("name"),
		Value: []byte("flydb"),
		Type:  Normal,
	}
	buf1, size := EncodeRecord(record1)
	assert.NotNil(t, buf1)
	assert.Greater(t, size, int64(5))

	// value is null
	record2 := &Record{
		Key:  []byte("name"),
		Type: Normal,
	}
	buf2, size2 := EncodeRecord(record2)
	assert.NotNil(t, buf2)
	assert.Greater(t, size2, int64(5))

	// Deleted condition
	record3 := &Record{
		Key:   []byte("name"),
		Value: []byte("flydb"),
		Type:  Deleted,
	}
	buf3, size3 := EncodeRecord(record3)
	assert.NotNil(t, buf3)
	assert.Greater(t, size3, int64(5))
}

func TestDecodeRecord(t *testing.T) {
	headerBuf := []byte{98, 201, 3, 114, 0, 8, 10}
	header, size := decodeRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(1912850786), header.crc)
	assert.Equal(t, Normal, header.recordType)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	header2, size2 := decodeRecordHeader(headerBuf2)
	assert.NotNil(t, header2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), header2.crc)
	assert.Equal(t, Normal, header2.recordType)
	assert.Equal(t, uint32(4), header2.keySize)
	assert.Equal(t, uint32(0), header2.valueSize)

	headerBuf3 := []byte{13, 133, 166, 233, 1, 8, 10}
	header3, size3 := decodeRecordHeader(headerBuf3)
	t.Log(header3)
	assert.NotNil(t, header3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(3920004365), header3.crc)
	assert.Equal(t, Deleted, header3.recordType)
	assert.Equal(t, uint32(4), header3.keySize)
	assert.Equal(t, uint32(5), header3.valueSize)
}

func TestGetRecordCRC(t *testing.T) {
	record1 := &Record{
		Key:   []byte("name"),
		Value: []byte("flydb"),
		Type:  Normal,
	}
	headerBuf := []byte{98, 201, 3, 114, 0, 8, 10}
	crc := calRecordCRC(record1, headerBuf[crc32.Size:])
	assert.Equal(t, uint32(1912850786), crc)

	record2 := &Record{
		Key:  []byte("name"),
		Type: Normal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := calRecordCRC(record2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	record3 := &Record{
		Key:   []byte("name"),
		Value: []byte("flydb"),
		Type:  Deleted,
	}
	headerBuf3 := []byte{13, 133, 166, 233, 1, 8, 10}
	crc3 := calRecordCRC(record3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(3920004365), crc3)

}