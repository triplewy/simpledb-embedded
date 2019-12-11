package db

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
)

// memTable is struct for Write-Ahead-Log and memtable
type memTable struct {
	table   *avlTree
	wal     *os.File
	walName string
	size    int
}

// newMemTable creates a file for the WAL and a new Memtable
func newMemTable(directory string, id string) (mt *memTable, maxCommitTs uint64, err error) {
	err = os.MkdirAll(filepath.Join(directory, "memtables"), dirPerm)
	if err != nil {
		return nil, 0, err
	}
	mt = &memTable{
		table:   newAVLTree(),
		wal:     nil,
		walName: filepath.Join(directory, "memtables", id),
		size:    0,
	}
	maxCommitTs, err = mt.RecoverWAL()
	if err != nil {
		return nil, 0, err
	}
	err = mt.SetWAL()
	if err != nil {
		return nil, 0, err
	}
	return mt, maxCommitTs, nil
}

func (mt *memTable) SetWAL() error {
	f, err := os.OpenFile(mt.walName, os.O_APPEND|os.O_WRONLY, filePerm)
	if err != nil {
		f.Close()
		return err
	}
	mt.wal = f
	return nil
}

// Write first appends a batch of writes to WAL then inserts them all into in-memory table
func (mt *memTable) Write(entries []*Entry) error {
	data := []byte{}
	for _, entry := range entries {
		data = append(data, encodeEntry(entry)...)
	}
	err := mt.AppendWAL(data)
	if err != nil {
		return err
	}
	// Put entries into memory structure after append to WAL to ensure consistency
	for _, entry := range entries {
		mt.table.Put(entry)
	}
	mt.size += len(data)
	return nil
}

// AppendWAL encodes an lsmDataEntry into bytes and appends to the WAL
func (mt *memTable) AppendWAL(data []byte) error {
	numBytes, err := mt.wal.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return newErrWriteUnexpectedBytes(mt.walName)
	}
	err = mt.wal.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (mt *memTable) Truncate() error {
	err := mt.wal.Close()
	if err != nil {
		return err
	}
	err = mt.truncate()
	if err != nil {
		return err
	}
	err = mt.SetWAL()
	if err != nil {
		return err
	}
	mt.table = newAVLTree()
	mt.size = 0
	return nil
}

func (mt *memTable) truncate() error {
	f, err := os.OpenFile(mt.walName, os.O_TRUNC, filePerm)
	defer f.Close()
	if err != nil {
		return err
	}
	ret, err := f.Seek(0, 0)
	if err != nil {
		return err
	}
	if ret != 0 {
		return errors.New("Seek did not go to 0")
	}
	return nil
}

// RecoverWAL reads the WAL and repopulates the memtable
func (mt *memTable) RecoverWAL() (maxCommitTs uint64, err error) {
	f, err := os.OpenFile(mt.walName, os.O_CREATE|os.O_EXCL, filePerm)
	defer f.Close()
	if err != nil {
		if os.IsExist(err) {
			data, err := ioutil.ReadFile(mt.walName)
			if err != nil {
				return 0, err
			}
			mt.size = len(data)
			entries := []*Entry{}
			i := 0
			for i < len(data) {
				if i+4 > len(data) {
					break
				}
				entrySize := binary.LittleEndian.Uint32(data[i : i+4])
				i += 4
				if i+int(entrySize) > len(data) {
					return 0, newErrBadFormattedSST()
				}
				if entrySize == 0 {
					break
				}
				entry, err := decodeEntry(data[i : i+int(entrySize)])
				if err != nil {
					return 0, err
				}
				i += int(entrySize)
				entries = append(entries, entry)
			}
			for _, entry := range entries {
				mt.table.Put(entry)
				if entry.ts > maxCommitTs {
					maxCommitTs = entry.ts
				}
			}
			return maxCommitTs, nil
		}
		return 0, err
	}
	return 0, nil
}
