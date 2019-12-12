package db

import "unicode"

// Read returns Attributes from the corresponding entry from the DB
func (db *DB) Read(key string, attributes []string) (*Entry, error) {
	var result *Entry
	err := db.ViewTxn(func(txn *Txn) error {
		entry, err := txn.Read(key)
		if err != nil {
			return err
		}
		result = entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	values := make(map[string]*Value)
	for _, name := range attributes {
		if value, ok := result.Attributes[name]; ok {
			values[name] = value
		} else {
			values[name] = nil
		}
	}
	result.Attributes = values
	return result, nil
}

// Scan takes a key and finds all entries that are greater than or equal to that key
func (db *DB) Scan(key string, attributes []string) (result []*Entry, err error) {
	maxRunes := []rune{}
	for i := 0; i < KeySize; i++ {
		maxRunes = append(maxRunes, unicode.MaxASCII)
	}
	maxKey := string(maxRunes)
	err = db.ViewTxn(func(txn *Txn) error {
		entries, err := txn.Scan(key, maxKey)
		if err != nil {
			return err
		}
		result = entries
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, entry := range result {
		values := make(map[string]*Value)
		for _, name := range attributes {
			if value, ok := entry.Attributes[name]; ok {
				values[name] = value
			} else {
				values[name] = nil
			}
		}
		entry.Attributes = values
	}
	return result, err
}

// Update updates certain Attributes in an entry
func (db *DB) Update(key string, values map[string]*Value) error {
	exists, err := db.checkPrimaryKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return newErrKeyNotFound()
	}
	err = db.UpdateTxn(func(txn *Txn) error {
		entry, err := txn.Read(key)
		if err != nil {
			return err
		}
		for name, value := range values {
			entry.Attributes[name] = value
		}
		txn.Write(key, entry.Attributes)
		return nil
	})
	return err
}

// Insert first checks if the key exists and if not, it inserts the new entry into the DB
func (db *DB) Insert(key string, values map[string]*Value) error {
	exists, err := db.checkPrimaryKey(key)
	if err != nil {
		return err
	}
	if exists {
		return newErrKeyAlreadyExists(key)
	}
	err = db.UpdateTxn(func(txn *Txn) error {
		txn.Write(key, values)
		return nil
	})
	return err
}

// Delete deletes an entry from the DB. If no entry with key exists, no error is thrown
func (db *DB) Delete(key string) error {
	err := db.UpdateTxn(func(txn *Txn) error {
		txn.Delete(key)
		return nil
	})
	return err
}
