package db

// Txn is Transaction struct for Optimistic Concurrency Control.
type Txn struct {
	db *DB

	startTs  uint64
	commitTs uint64

	writeCache map[string]*Entry
	readSet    map[string]uint64
}

// StartTxn returns a new Txn to perform ops on
func (db *DB) StartTxn() *Txn {
	return &Txn{
		db:         db,
		startTs:    db.oracle.requestStart(),
		writeCache: make(map[string]*Entry),
		readSet:    make(map[string]uint64),
	}
}

// Read gets value for a key from the DB and updates the txn readSet
func (txn *Txn) Read(key string) (*Entry, error) {
	entry, err := txn.db.read(key, txn.startTs)
	if err != nil {
		return nil, err
	}
	txn.readSet[key] = entry.ts
	return entry, nil
}

// Write updates the write cache of the txn
func (txn *Txn) Write(key string, attributes map[string]*Value) {
	txn.writeCache[key] = &Entry{
		Key:        key,
		Attributes: attributes,
	}
}

// Delete updates the write cache of the txn
func (txn *Txn) Delete(key string) {
	txn.writeCache[key] = &Entry{
		Key:        key,
		Attributes: nil,
	}
}

// Scan gets a range of values from a start key to an end key from the DB and updates the txn readSet
func (txn *Txn) Scan(startKey, endKey string) ([]*Entry, error) {
	kvs, err := txn.db.scan(startKey, endKey, txn.startTs)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		txn.readSet[kv.Key] = kv.ts
	}
	return kvs, nil
}

// Exists checks if key exists in the db
func (txn *Txn) Exists(key string) (bool, error) {
	_, err := txn.Read(key)
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

// Commit sends the txn's read and write set to the oracle for commit
func (txn *Txn) Commit() error {
	if len(txn.writeCache) == 0 {
		return nil
	}
	return txn.db.oracle.commit(txn.readSet, txn.writeCache)
}
