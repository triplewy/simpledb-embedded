package db

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"
)

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, u)
	return buf
}

// deleteData deletes all data from database
func deleteData(directory string) error {
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		return nil
	}
	dirs, err := ioutil.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, f := range dirs {
		os.RemoveAll(path.Join([]string{directory, f.Name()}...))
	}
	return nil
}
