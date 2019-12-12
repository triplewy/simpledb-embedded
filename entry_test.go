package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func setupEntry() (map[string]interface{}, *Entry, error) {
	attributes := make(map[string]interface{})
	attributes["id"] = uuid.New().String()
	attributes["balance"] = float64(100.0)
	attributes["isUser"] = true
	attributes["likes"] = int64(10)
	b, err := json.Marshal(map[string]string{"test": "test"})
	if err != nil {
		return nil, nil, err
	}
	attributes["info"] = b
	entry, err := createEntry(uint64(0), "test", attributes)
	if err != nil {
		return nil, nil, err
	}
	return attributes, entry, nil
}

func TestEntryCreate(t *testing.T) {
	attributes, entry, err := setupEntry()
	if err != nil {
		t.Fatalf("Error setting up entry: %v\n", err)
	}
	if entry.ts != 0 {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	if entry.Key != "test" {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	for name, v1 := range attributes {
		if v2, ok := entry.Attributes[name]; ok {
			v, err := ParseValue(v2)
			if err != nil {
				t.Fatalf("Error parsing value: %v\n", err)
			}
			switch v1.(type) {
			case []byte:
				if !bytes.Equal(v.([]byte), v1.([]byte)) {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			default:
				if v != v1 {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			}
		} else {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
	}
}

func TestEntryEncode(t *testing.T) {
	_, entry, err := setupEntry()
	if err != nil {
		t.Fatalf("Error setting up entry: %v\n", err)
	}
	data := encodeEntry(entry)
	totalSize := binary.LittleEndian.Uint32(data[0:4])
	if int(totalSize)+4 != len(data) {
		t.Fatalf("Wrong size in entry encode\n")
	}
}

func TestEntryDecode(t *testing.T) {
	attributes, entry, err := setupEntry()
	if err != nil {
		t.Fatalf("Error setting up entry: %v\n", err)
	}
	data := encodeEntry(entry)
	result, err := decodeEntry(data[4:])
	if err != nil {
		t.Fatalf("Error decoding entry: %v\n", err)
	}
	if result.ts != 0 {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	if result.Key != "test" {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	for name, v1 := range attributes {
		if v2, ok := result.Attributes[name]; ok {
			v, err := ParseValue(v2)
			if err != nil {
				t.Fatalf("Error parsing value: %v\n", err)
			}
			switch v1.(type) {
			case []byte:
				if !bytes.Equal(v.([]byte), v1.([]byte)) {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			default:
				if v != v1 {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			}
		} else {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
	}
}

func TestEntryWrite(t *testing.T) {
	attributes, entry, err := setupEntry()
	entries := []*Entry{}
	for i := 0; i < 100; i++ {
		entries = append(entries, entry)
	}
	dataBlocks, _, _, _, err := writeEntries(entries)
	if err != nil {
		t.Fatalf("Error writing entries: %v\n", err)
	}
	result, err := decodeEntries(dataBlocks)
	if err != nil {
		t.Fatalf("Error decoding entries: %v\n", err)
	}
	if len(result) != len(entries) {
		t.Fatalf("Length result does not match length entries. Expected: %d, Got: %d\n", len(entries), len(result))
	}
	for _, entry := range result {
		if entry.ts != 0 {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
		if entry.Key != "test" {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
		for name, v1 := range attributes {
			if v2, ok := entry.Attributes[name]; ok {
				v, err := ParseValue(v2)
				if err != nil {
					t.Fatalf("Error parsing value: %v\n", err)
				}
				switch v1.(type) {
				case []byte:
					if !bytes.Equal(v.([]byte), v1.([]byte)) {
						t.Fatalf("Incorrect entry: %v\n", entry)
					}
				default:
					if v != v1 {
						t.Fatalf("Expected: %v, Got: %v\n", v1, v)
					}
				}
			} else {
				t.Fatalf("Incorrect entry: %v\n", entry)
			}
		}
	}
}
