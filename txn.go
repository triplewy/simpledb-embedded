package db

// Txn is Transaction struct for Optimistic Concurrency Control.
type Txn struct {
	db *DB

	startTs  uint64
	commitTs uint64

	writeCache map[string]*Entry
	readSet    map[string]uint64
}

// NewTxn returns a new Txn to perform ops on
func (db *DB) NewTxn() *Txn {
	startTs := db.oracle.requestStart()
	return &Txn{
		db:         db,
		startTs:    startTs,
		writeCache: make(map[string]*Entry),
		readSet:    make(map[string]uint64),
	}
}

// ViewTxn implements a read only transaction to the DB. Ensures read only since it does not commit at end
func (db *DB) ViewTxn(fn func(txn *Txn) error) error {
	txn := db.NewTxn()
	return fn(txn)
}

// UpdateTxn implements a read and write only transaction to the DB
func (db *DB) UpdateTxn(fn func(txn *Txn) error) error {
	txn := db.NewTxn()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

// Read gets value for a key from the DB and updates the txn readSet
func (txn *Txn) Read(primaryKey, rangeKey []byte) (*Entry, error) {
	key := append(primaryKey, rangeKey...)
	entry, err := txn.db.read(primaryKey, rangeKey, txn.startTs)
	if err != nil {
		return nil, err
	}
	txn.readSet[string(key)] = entry.ts
	return entry, nil
}

// Write updates the write cache of the txn
func (txn *Txn) Write(primaryKey, rangeKey []byte, Fields map[string]*Value) {
	key := append(primaryKey, rangeKey...)
	txn.writeCache[string(key)] = &Entry{
		PrimaryKey: primaryKey,
		RangeKey:   rangeKey,
		totalKey:   key,
		Fields:     Fields,
	}
}

// Delete updates the write cache of the txn
func (txn *Txn) Delete(primaryKey, rangeKey []byte) {
	key := append(primaryKey, rangeKey...)
	txn.writeCache[string(key)] = &Entry{
		PrimaryKey: primaryKey,
		RangeKey:   rangeKey,
		totalKey:   key,
		Fields:     nil,
	}
}

// Query finds entries based on primary key
func (txn *Txn) Query(primaryKey []byte) ([]*Entry, error) {

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

// Commit sends the txn's read and write set to the oracle for commit
func (txn *Txn) Commit() error {
	if len(txn.writeCache) == 0 {
		return nil
	}
	return txn.db.oracle.commit(txn.readSet, txn.writeCache)
}
