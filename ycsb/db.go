package ycsb

import (
	"context"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	simpledb "github.com/triplewy/simpledb-embedded"
)

type simpleDBCreator struct {
}

type simpleDB struct {
	db *simpledb.DB
}

type contextKey string

const stateKey = contextKey("simpledb")

type simpleDBState struct {
}

func (c simpleDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db, err := simpledb.NewDB("data")
	if err != nil {
		return nil, err
	}
	return &simpleDB{db: db}, nil
}

func (s *simpleDB) Close() error {
	s.db.Close()
	return nil
}

func (s *simpleDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (s *simpleDB) CleanupThread(_ context.Context) {
	return
}

func (s *simpleDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	txn := s.db.StartTxn()
	entry, err := txn.Read(table + key)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte)
	for name, value := range entry.Attributes {
		result[name] = value.Data
	}
	return result, nil
}

func (s *simpleDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	txn := s.db.StartTxn()
	entries, err := txn.Scan(table+startKey, table+startKey)
	if err != nil {
		return nil, err
	}
	result := []map[string][]byte{}
	for _, entry := range entries {
		attributes := make(map[string][]byte)
		for name, value := range entry.Attributes {
			attributes[name] = value.Data
		}
		result = append(result, attributes)
	}
	return result, nil
}

func (s *simpleDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	txn := s.db.StartTxn()
	entry, err := txn.Read(table + key)
	if err != nil {
		return err
	}
	for name, value := range values {
		v, err := simpledb.CreateValue(value)
		if err != nil {
			return err
		}
		entry.Attributes[name] = v
	}
	txn.Write(entry.Key, entry.Attributes)
	return txn.Commit()
}

func (s *simpleDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	txn := s.db.StartTxn()
	exists, err := txn.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("key: %v already exists in db", table+key)
	}
	attributes := make(map[string]*simpledb.Value)
	for name, value := range values {
		v, err := simpledb.CreateValue(value)
		if err != nil {
			return err
		}
		attributes[name] = v
	}
	txn.Write(table+key, attributes)
	return txn.Commit()
}

func (s *simpleDB) Delete(ctx context.Context, table string, key string) error {
	txn := s.db.StartTxn()
	txn.Delete(table + key)
	return txn.Commit()
}

func init() {
	ycsb.RegisterDBCreator("simpledb", simpleDBCreator{})
}
