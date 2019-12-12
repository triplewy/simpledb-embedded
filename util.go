package db

import (
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

func checkKeySize(primaryKey, rangeKey []byte) error {
	if len(primaryKey)+len(rangeKey) > KeySize {
		return newErrExceedMaxKeySize(append(primaryKey, rangeKey...))
	}
	return nil
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
