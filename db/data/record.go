package data

import (
	"encoding/binary"
	"hash/crc32"
)

type RecordType = byte

// crc type KeySize ValueSize
// 4 +  1 +   5   +    5    (byte)
const maxRecordHeaderSize = binary.MaxVarintLen32*2 + 5
const (
	Normal RecordType = iota
	Deleted
	Finished
)

type Record struct {
	Key   []byte
	Value []byte
	Type  RecordType
}

type RecordHeader struct {
	crc        uint32
	recordType RecordType
	keySize    uint32
	valueSize  uint32
}

// RecordPst represents the in-memory index of data,
// mainly describing the location of data on disk.
type RecordPst struct {
	Fid    uint32 // File ID: Indicates which file the data is stored in
	Offset int64  // Offset: Indicates the position in the data file where the data is stored
}

// EncodeRecordPst encodes the position information of a log record.
func EncodeRecordPst(pst *RecordPst) []byte {
	buf := make([]byte, binary.MaxVarintLen16+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pst.Fid)) // Encode file ID
	index += binary.PutVarint(buf[index:], pst.Offset)     // Encode offset
	return buf[:index]
}

// DecodeRecordPst decodes the position information from a byte buffer.
func DecodeRecordPst(buf []byte) *RecordPst {
	var index = 0
	fileID, n := binary.Varint(buf[index:]) // Decode file ID
	index += n
	offset, _ := binary.Varint(buf[index:]) // Decode offset
	return &RecordPst{
		Fid:    uint32(fileID), // Convert file ID to uint32
		Offset: offset,         // Assign offset
	}
}

func EncodeRecord(record *Record) ([]byte, int64) {
	header := make([]byte, maxRecordHeaderSize)

	// store the record type at fifth byte
	header[4] = record.Type

	offset := 5

	offset += binary.PutVarint(header[offset:], int64(len(record.Key)))
	offset += binary.PutVarint(header[offset:], int64(len(record.Value)))

	size := offset + len(record.Key) + len(record.Value)

	encBytes := make([]byte, size)
	copy(encBytes[:offset], header[:offset])
	copy(encBytes[offset:], record.Key)
	copy(encBytes[offset+len(record.Key):], record.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])

	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

func decodeRecordHeader(data []byte) (*RecordHeader, int64) {
	if len(data) <= 4 {
		return nil, 0
	}

	header := &RecordHeader{
		crc:        binary.LittleEndian.Uint32(data[:4]),
		recordType: RecordType(data[4]),
	}

	offset := 5
	keySize, n := binary.Varint(data[offset:])
	header.keySize = uint32(keySize)
	offset += n
	valueSize, n := binary.Varint(data[offset:])
	header.valueSize = uint32(valueSize)
	offset += n

	return header, int64(offset)

}
func calRecordCRC(record *Record, header []byte) uint32 {
	if record == nil {
		return 0
	}
	// Calculate CRC checksum for the header
	crc := crc32.ChecksumIEEE(header[:])
	// Update CRC checksum with the key
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	// Update CRC checksum with the value
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)

	return crc
}
