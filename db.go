package db

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
)

// DB is struct for database
type DB struct {
	oracle    *oracle
	mutable   *memTable
	immutable *memTable
	lsm       *lsm

	writeChan chan *writeRequest
	flushChan chan *memTable
	close     chan struct{}
}

type writeRequest struct {
	entries []*Entry
	errChan chan error
}

// NewDB creates a new database by instantiating the lsm and Value Log
func NewDB(directory string) (*DB, error) {
	err := os.MkdirAll(directory, dirPerm)
	if err != nil {
		return nil, err
	}
	lsm, err := newLSM(directory)
	if err != nil {
		return nil, err
	}
	memtable1, maxCommitTs1, err := newMemTable(directory, "1")
	if err != nil {
		return nil, err
	}
	memtable2, maxCommitTs2, err := newMemTable(directory, "2")
	if err != nil {
		return nil, err
	}
	maxCommitTs := maxCommitTs1
	if maxCommitTs2 > maxCommitTs {
		maxCommitTs = maxCommitTs2
	}
	db := &DB{
		mutable:   memtable1,
		immutable: memtable2,

		lsm: lsm,

		writeChan: make(chan *writeRequest),
		flushChan: make(chan *memTable),
		close:     make(chan struct{}, 1),
	}
	oracle := newOracle(maxCommitTs+1, db)
	db.oracle = oracle

	go db.run()
	go db.runFlush()

	return db, nil
}

// write inserts multiple entries into DB
func (db *DB) write(entries []*Entry) error {
	errChan := make(chan error, 1)
	req := &writeRequest{
		entries: entries,
		errChan: errChan,
	}
	db.writeChan <- req
	return <-errChan
}

// get retrieves Fields for a given key or returns key not found
func (db *DB) read(primaryKey, rangeKey []byte, ts uint64) (*Entry, error) {
	err := checkKeySize(primaryKey, rangeKey)
	if err != nil {
		return nil, err
	}
	key := append(primaryKey, rangeKey...)
	entry := db.mutable.table.Find(string(key), ts)
	if entry != nil {
		if entry.Fields == nil {
			return nil, newErrKeyNotFound()
		}
		return entry, nil
	}
	entry = db.immutable.table.Find(string(key), ts)
	if entry != nil {
		if entry.Fields == nil {
			return nil, newErrKeyNotFound()
		}
		return entry, nil
	}
	entry, err = db.lsm.Read(primaryKey, rangeKey, ts)
	if err != nil {
		return nil, err
	}
	if entry.Fields == nil {
		return nil, newErrKeyNotFound()
	}
	return entry, nil
}

func (db *DB) query(primaryKey []byte, startKey, endKey []byte, ts uint64) ([]*Entry, error) {
	err := checkKeySize(primaryKey, startKey)
	if err != nil {
		return nil, err
	}
	err = checkKeySize(primaryKey, endKey)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(startKey, endKey) > 0 {
		return nil, errors.New("Start Key is greater than End Key")
	}
	keyRange := &keyRange{
		startKey: string(append(primaryKey, startKey...)),
		endKey:   string(append(primaryKey, endKey...)),
	}
	all := []*Entry{}
	all = append(all, db.mutable.table.Scan(keyRange, ts)...)
	all = append(all, db.immutable.table.Scan(keyRange, ts)...)
}

// range finds all key, value pairs within the given range of keys
func (db *DB) scan(startKey, endKey string, ts uint64) ([]*Entry, error) {
	if len(startKey) > KeySize {
		return nil, newErrExceedMaxKeySize(startKey)
	}
	if len(endKey) > KeySize {
		return nil, newErrExceedMaxKeySize(endKey)
	}
	if startKey > endKey {
		return nil, errors.New("Start Key is greater than End Key")
	}
	keyRange := &keyRange{startKey: startKey, endKey: endKey}

	all := []*Entry{}
	all = append(all, db.mutable.table.Scan(keyRange, ts)...)
	all = append(all, db.immutable.table.Scan(keyRange, ts)...)

	entries, err := db.lsm.Scan(keyRange, ts)
	if err != nil {
		return nil, err
	}
	all = append(all, entries...)

	// Convert slice of entries into a set of entries
	resultMap := make(map[string]*Entry)
	for _, entry := range all {
		if value, ok := resultMap[entry.Key]; !ok {
			resultMap[entry.Key] = entry
		} else {
			if value.ts < entry.ts {
				resultMap[entry.Key] = entry
			}
		}
	}

	result := []*Entry{}
	for _, entry := range resultMap {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result, nil
}

func (db *DB) checkPrimaryKey(key string) (bool, error) {
	_, err := db.read(key, math.MaxUint64)
	if err != nil {
		switch err.(type) {
		case *ErrKeyNotFound:
			return false, nil
		default:
			return false, err
		}
	}
	return true, nil
}

// Flush takes all entries from the in-memory table and sends them to lsm
func (db *DB) flush(mt *memTable) error {
	entries := mt.table.Inorder()
	// Flush to lsm
	err := db.lsm.Write(entries)
	if err != nil {
		return err
	}
	// Truncate the WAL
	err = mt.Truncate()
	if err != nil {
		return err
	}
	return nil
}

// Close gracefully closes the database
func (db *DB) Close() {
	db.close <- struct{}{}
}

// ForceClose immediately shuts down the database. Good for testing
func (db *DB) forceClose() {
	os.Exit(0)
}

func (db *DB) run() {
	for {
		select {
		case req := <-db.writeChan:
			err := db.mutable.Write(req.entries)
			if err != nil {
				req.errChan <- err
			} else {
				if db.mutable.size > MemTableSize {
					db.flushChan <- db.mutable
					db.mutable, db.immutable = db.immutable, db.mutable
				}
				req.errChan <- nil
			}
		case <-db.close:
			db.lsm.Close()
			return
		}
	}
}

func (db *DB) runFlush() {
	for {
		select {
		case mt := <-db.flushChan:
			err := db.flush(mt)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}
