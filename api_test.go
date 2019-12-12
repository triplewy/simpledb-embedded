package db

import (
	"strconv"
	"testing"
)

func TestAPIInsert(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	value, err := CreateValue("test")
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Insert("test", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	value, err = CreateValue("another test")
	err = db.Insert("test", map[string]*Value{"value": value})
	if _, ok := err.(*ErrKeyAlreadyExists); !ok {
		t.Fatalf("Expected: ErrKeyAlreadyExists, Got: %v\n", err)
	}
	entry, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if value, ok := entry.Attributes["value"]; ok {
		v, err := ParseValue(value)
		if err != nil {
			t.Fatalf("Error parsing value: %v\n", err)
		}
		if v.(string) != "test" {
			t.Fatalf("Got wrong read\n")
		}
	} else {
		t.Fatalf("Error entry does not have 'value' attribute\n")
	}
}

func TestAPIUpdate(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	value, err := CreateValue("test")
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Update("test", map[string]*Value{"value": value})
	if _, ok := err.(*ErrKeyNotFound); !ok {
		t.Fatalf("Expected: ErrKeyNotFound, Got: %v\n", err)
	}
	value, err = CreateValue("test")
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Insert("test", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	value, err = CreateValue("another test")
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Update("test", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error updating db: %v\n", err)
	}
	entry, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if value, ok := entry.Attributes["value"]; ok {
		v, err := ParseValue(value)
		if err != nil {
			t.Fatalf("Error parsing value: %v\n", err)
		}
		if v.(string) != "another test" {
			t.Fatalf("Got wrong read\n")
		}
	} else {
		t.Fatalf("Error entry does not have 'value' attribute\n")
	}
}

func TestAPIDelete(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	err = db.Delete("test")
	if err != nil {
		t.Fatalf("Error deleting from db: %v\n", err)
	}
	value, err := CreateValue("test")
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Insert("test", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entry, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if value, ok := entry.Attributes["value"]; ok {
		v, err := ParseValue(value)
		if err != nil {
			t.Fatalf("Error parsing value: %v\n", err)
		}
		if v.(string) != "test" {
			t.Fatalf("Got wrong read\n")
		}
	} else {
		t.Fatalf("Error entry does not have 'value' attribute\n")
	}
	err = db.Delete("test")
	if err != nil {
		t.Fatalf("Error deleting from db: %v\n", err)
	}
	entry, err = db.Read("test", []string{"value"})
	if _, ok := err.(*ErrKeyNotFound); !ok {
		t.Fatalf("Expected: ErrKeyNotFound, Got: %v\n", err)
	}
}

func TestAPIRead(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	values := make(map[string]*Value)
	for i := int64(1); i < 4; i++ {
		value, err := CreateValue(i)
		if err != nil {
			t.Fatalf("Error creating value: %v\n", err)
		}
		values[strconv.FormatInt(i, 10)] = value
	}
	err = db.Insert("test", values)
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entry, err := db.Read("test", []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	for i := 1; i < 5; i++ {
		key := strconv.Itoa(i)
		if i == 4 {
			if entry.Attributes[key] != nil {
				t.Fatalf("values[1] Expected: nil, Got: %v\n", entry.Attributes[key])
			}
		} else {
			value, err := ParseValue(entry.Attributes[key])
			if err != nil {
				t.Fatalf("Error parsing value: %v\n", err)
			}
			if value.(int64) != int64(i) {
				t.Fatalf("values[1] Expected: %v, Got: %d\n", []byte(key), value.(int64))
			}
		}
	}
	value, err := CreateValue(int64(4))
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Update("test", map[string]*Value{"4": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entry, err = db.Read("test", []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	for i := 1; i < 5; i++ {
		key := strconv.Itoa(i)
		value, err := ParseValue(entry.Attributes[key])
		if err != nil {
			t.Fatalf("Error parsing value: %v\n", err)
		}
		if value.(int64) != int64(i) {
			t.Fatalf("values[1] Expected: %v, Got: %d\n", []byte(key), value.(int64))
		}
	}
}

func TestAPIScan(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	value, err := CreateValue("test")
	if err != nil {
		t.Fatalf("Error creating value: %v\n", err)
	}
	err = db.Insert("test", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entries, err := db.Scan("0", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Scan length, Expected: 1, Got: %d\n", len(entries))
	}
	v, err := ParseValue(entries[0].Attributes["value"])
	if err != nil {
		t.Fatalf("Error parsing value: %v\n", err)
	}
	if v.(string) != "test" {
		t.Fatalf("Wrong value for scan\n")
	}
	entries, err = db.Scan("u", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Scan length, Expected: 0, Got: %d\n", len(entries))
	}
	err = db.Insert("z", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	err = db.Insert("zz999", map[string]*Value{"value": value})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entries, err = db.Scan("0", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Scan length, Expected: 1, Got: %d\n", len(entries))
	}
	for _, entry := range entries {
		v, err := ParseValue(entry.Attributes["value"])
		if err != nil {
			t.Fatalf("Error parsing value: %v\n", err)
		}
		if v.(string) != "test" {
			t.Fatalf("Wrong value for scan\n")
		}
	}
}
