package db

import (
	"encoding/binary"
	"math"
)

// Entry represents a row in the db where a key is mapped to multiple Fields
type Entry struct {
	ts         uint64
	PrimaryKey []byte
	RangeKey   []byte
	totalKey   []byte
	Fields     map[string]*Value
}

// Value combines a slice of bytes with a data type in order to parse data
type Value struct {
	DataType uint8
	Data     []byte
}

// CreateValue converts an arbitrary typed value into a Value struct
func CreateValue(value interface{}) (*Value, error) {
	switch v := value.(type) {
	case bool:
		value := []byte{0}
		if v {
			value = []byte{1}
		}
		return &Value{DataType: Bool, Data: value}, nil
	case int64:
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, uint64(v))
		return &Value{DataType: Int, Data: value}, nil
	case uint64:
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, v)
		return &Value{DataType: Uint, Data: value}, nil
	case float64:
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, math.Float64bits(v))
		return &Value{DataType: Float, Data: value}, nil
	case string:
		return &Value{DataType: String, Data: []byte(v)}, nil
	case []byte:
		return &Value{DataType: Bytes, Data: v}, nil
	case nil:
		return &Value{DataType: Tombstone, Data: []byte{}}, nil
	default:
		return nil, newErrNoTypeFound()
	}
}

// ParseValue converts a Value struct to an interface value
func ParseValue(value *Value) (interface{}, error) {
	data := value.Data
	switch value.DataType {
	case Bool:
		if len(data) != 1 {
			return nil, newErrParseValue(value)
		}
		if data[0] == byte(0) {
			return false, nil
		}
		if data[0] == byte(1) {
			return true, nil
		}
		return nil, newErrParseValue(value)
	case Int:
		if len(data) != 8 {
			return nil, newErrParseValue(value)
		}
		return int64(binary.LittleEndian.Uint64(data)), nil
	case Uint:
		if len(data) != 8 {
			return nil, newErrParseValue(value)
		}
		return binary.LittleEndian.Uint64(data), nil
	case Float:
		if len(data) != 8 {
			return nil, newErrParseValue(value)
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), nil
	case String:
		return string(data), nil
	case Bytes:
		return data, nil
	default:
		return nil, newErrParseValue(value)
	}
}

func createEntry(ts uint64, primaryKey, rangeKey []byte, fields map[string]interface{}) (*Entry, error) {
	err := checkKeySize(primaryKey, rangeKey)
	if err != nil {
		return nil, err
	}
	if len(fields) > MaxFields {
		return nil, newErrExceedMaxFields()
	}
	entry := &Entry{
		ts:         ts,
		PrimaryKey: primaryKey,
		RangeKey:   rangeKey,
		totalKey:   append(primaryKey, rangeKey...),
		Fields:     make(map[string]*Value),
	}
	for name, data := range fields {
		value, err := CreateValue(data)
		if err != nil {
			return nil, err
		}
		entry.Fields[name] = value
	}
	totalSize := 0
	for _, value := range entry.Fields {
		totalSize += len(value.Data)
		if totalSize > EntrySize {
			return nil, newErrExceedMaxEntrySize()
		}
	}
	return entry, nil
}

func encodeEntry(entry *Entry) (data []byte) {
	tsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBytes, entry.ts)
	pKeySizeBytes := uint8(len(entry.PrimaryKey))
	pKeyBytes := []byte(entry.PrimaryKey)
	rKeySizeBytes := uint8(len(entry.RangeKey))
	rKeyBytes := []byte(entry.RangeKey)
	FieldsBytes := []byte{}

	for name, value := range entry.Fields {
		nameSizeBytes := uint8(len(name))
		nameBytes := []byte(name)
		DataTypeBytes := value.DataType
		dataSizeBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(dataSizeBytes, uint16(len(value.Data)))
		dataBytes := value.Data

		fieldBytes := []byte{}
		fieldBytes = append(fieldBytes, nameSizeBytes)
		fieldBytes = append(fieldBytes, nameBytes...)
		fieldBytes = append(fieldBytes, DataTypeBytes)
		fieldBytes = append(fieldBytes, dataSizeBytes...)
		fieldBytes = append(fieldBytes, dataBytes...)

		FieldsBytes = append(FieldsBytes, fieldBytes...)
	}

	data = append(data, tsBytes...)
	data = append(data, pKeySizeBytes)
	data = append(data, pKeyBytes...)
	data = append(data, rKeySizeBytes)
	data = append(data, rKeyBytes...)
	data = append(data, FieldsBytes...)

	totalSize := uint32(len(data))
	totalSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalSizeBytes, totalSize)

	data = append(totalSizeBytes, data...)
	return data
}

func decodeEntry(data []byte) (*Entry, error) {
	const (
		tsBytes uint8 = iota
		keyBytes
		fieldBytes
	)
	fields := make(map[string]*Value)
	entry := &Entry{
		ts:         0,
		PrimaryKey: nil,
		RangeKey:   nil,
		Fields:     nil,
	}
	step := tsBytes
	i := 0
	for i < len(data) {
		switch step {
		case tsBytes:
			if i+8 > len(data) {
				return nil, newErrDecodeEntry()
			}
			entry.ts = binary.LittleEndian.Uint64(data[i : i+8])
			i += 8
			step = keyBytes
		case keyBytes:
			pKeySize := uint8(data[i])
			i++
			if i+int(pKeySize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			entry.PrimaryKey = data[i : i+int(pKeySize)]
			i += int(pKeySize)
			rKeySize := uint8(data[i])
			i++
			if i+int(rKeySize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			entry.RangeKey = data[i : i+int(rKeySize)]
			entry.totalKey = append(entry.PrimaryKey, entry.RangeKey...)
			i += int(rKeySize)
			step = fieldBytes
		case fieldBytes:
			fieldNameSize := uint8(data[i])
			i++
			if i+int(fieldNameSize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			fieldName := string(data[i : i+int(fieldNameSize)])
			i += int(fieldNameSize)
			fieldType := uint8(data[i])
			i++
			if i+2 > len(data) {
				return nil, newErrDecodeEntry()
			}
			fieldDataSize := binary.LittleEndian.Uint16(data[i : i+2])
			i += 2
			if i+int(fieldDataSize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			fieldData := data[i : i+int(fieldDataSize)]
			fields[fieldName] = &Value{DataType: fieldType, Data: fieldData}
			i += int(fieldDataSize)
		default:
			return nil, newErrDecodeEntry()
		}
	}
	if len(fields) > 0 {
		entry.Fields = fields
	}
	return entry, nil
}

func decodeEntries(data []byte) (entries []*Entry, err error) {
	for i := 0; i < len(data); i += BlockSize {
		block := data[i : i+BlockSize]
		j := 0
		for j < len(block) {
			if j+4 > len(block) {
				break
			}
			entrySize := binary.LittleEndian.Uint32(block[j : j+4])
			j += 4
			if j+int(entrySize) > len(block) {
				return nil, newErrBadFormattedSST()
			}
			if entrySize == 0 {
				break
			}
			entry, err := decodeEntry(block[j : j+int(entrySize)])
			if err != nil {
				return nil, err
			}
			j += int(entrySize)
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func writeEntries(entries []*Entry) (dataBlocks, indexBlock []byte, pBloom, rBloom *bloom, kr *keyRange, err error) {
	kr = &keyRange{
		startKey: string(entries[0].totalKey),
		endKey:   string(entries[len(entries)-1].totalKey),
	}
	pSet := make(map[string]struct{})
	rSet := make(map[string]struct{})

	block := make([]byte, BlockSize)
	currBlock := uint32(0)
	i := 0
	for index, entry := range entries {
		entryBytes := encodeEntry(entry)
		// Create new block if current entry overflows block
		if i+len(entryBytes) > BlockSize {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&indexEntry{
				key:   entries[index-1].totalKey,
				block: currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
			block = make([]byte, BlockSize)
			currBlock++
			i = 0
		}
		i += copy(block[i:], entryBytes)
		pSet[string(entry.PrimaryKey)] = struct{}{}
		rSet[string(entry.RangeKey)] = struct{}{}
		// If last entry, append data block and index entry
		if index == len(entries)-1 {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&indexEntry{
				key:   entry.totalKey,
				block: currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
		}
	}
	pBloom = newBloom(len(pSet))
	rBloom = newBloom(len(rSet))
	for key := range pSet {
		pBloom.Insert([]byte(key))
	}
	for key := range rSet {
		rBloom.Insert([]byte(key))
	}
	return dataBlocks, indexBlock, pBloom, rBloom, kr, nil
}

// indexEntry is struct that represents an entry into an lsm Index Block
type indexEntry struct {
	key   []byte
	block uint32
}

func encodeIndexEntry(entry *indexEntry) (data []byte) {
	data = append(data, uint8(len(entry.key)))
	data = append(data, entry.key...)
	blockBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockBytes, entry.block)
	data = append(data, blockBytes...)
	return data
}
