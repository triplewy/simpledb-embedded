package db

import (
	"encoding/binary"
	"math"
)

// Entry represents a row in the db where a key is mapped to multiple Attributes
type Entry struct {
	ts         uint64
	Key        string
	Attributes map[string]*Value
}

// Value combines a slice of bytes with a data type in order to parse data
type Value struct {
	DataType uint8
	Data     []byte
}

// CreateValue converts an interface value to type Value
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

// ParseValue converts type Value to an interface
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

// indexEntry is struct that represents an entry into an lsm Index Block
type indexEntry struct {
	key   string
	block uint32
}

func createEntry(ts uint64, key string, attributes map[string]interface{}) (*Entry, error) {
	if len(key) > KeySize {
		return nil, newErrExceedMaxKeySize(key)
	}
	if len(attributes) > MaxAttributes {
		return nil, newErrExceedMaxAttributes()
	}
	entry := &Entry{
		ts:         ts,
		Key:        key,
		Attributes: make(map[string]*Value),
	}
	for name, attribute := range attributes {
		value, err := CreateValue(attribute)
		if err != nil {
			return nil, err
		}
		entry.Attributes[name] = value
	}
	totalSize := 0
	for _, value := range entry.Attributes {
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
	keySizeBytes := uint8(len(entry.Key))
	keyBytes := []byte(entry.Key)
	AttributesBytes := []byte{}

	for name, value := range entry.Attributes {
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

		AttributesBytes = append(AttributesBytes, fieldBytes...)
	}

	data = append(data, tsBytes...)
	data = append(data, keySizeBytes)
	data = append(data, keyBytes...)
	data = append(data, AttributesBytes...)

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
	Attributes := make(map[string]*Value)
	entry := &Entry{
		ts:         0,
		Key:        "",
		Attributes: nil,
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
			keySize := uint8(data[i])
			i++
			if i+int(keySize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			entry.Key = string(data[i : i+int(keySize)])
			i += int(keySize)
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
			Attributes[fieldName] = &Value{DataType: fieldType, Data: fieldData}
			i += int(fieldDataSize)
		default:
			return nil, newErrDecodeEntry()
		}
	}
	if len(Attributes) > 0 {
		entry.Attributes = Attributes
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

func writeEntries(entries []*Entry) (dataBlocks, indexBlock []byte, bloom *bloom, kr *keyRange, err error) {
	kr = &keyRange{
		startKey: entries[0].Key,
		endKey:   entries[len(entries)-1].Key,
	}
	bloom = newBloom(len(entries))
	block := make([]byte, BlockSize)
	currBlock := uint32(0)
	i := 0
	for index, entry := range entries {
		entryBytes := encodeEntry(entry)
		// Create new block if current entry overflows block
		if i+len(entryBytes) > BlockSize {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&indexEntry{
				key:   entries[index-1].Key,
				block: currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
			block = make([]byte, BlockSize)
			currBlock++
			i = 0
		}
		i += copy(block[i:], entryBytes)
		bloom.Insert(entry.Key)
		// If last entry, append data block and index entry
		if index == len(entries)-1 {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&indexEntry{
				key:   entry.Key,
				block: currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
		}
	}
	return dataBlocks, indexBlock, bloom, kr, nil
}

func encodeIndexEntry(entry *indexEntry) (data []byte) {
	data = append(data, uint8(len(entry.key)))
	data = append(data, []byte(entry.key)...)
	blockBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockBytes, entry.block)
	data = append(data, blockBytes...)
	return data
}
