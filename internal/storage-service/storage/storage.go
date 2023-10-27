package storage

import (
	"errors"
	"github.com/google/uuid"
	"log"
	"path/filepath"
)

type Storage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}

type StorageImpl struct {
	MemTable             MemTable
	SsTables             *[]SsTable
	SsTableSegmentLength int64
	SsTableDir           string
}

func (storage StorageImpl) Set(key string, value string) error {
	err := storage.MemTable.Add(key, value)
	if err != nil {
		log.Printf("Copy MemTable to the ssTable")
		var id = uuid.New()
		filePath := filepath.Join(storage.SsTableDir, id.String())
		var newTable = SsTable{dirPath: filePath + ".bin", segmentLength: storage.SsTableSegmentLength, sparseIndex: make(map[string]int64),
			id: uuid.New()}
		newTable.Init(storage.MemTable)
		storage.MemTable.Clear()
		*storage.SsTables = append(*storage.SsTables, newTable)
	}
	return nil
}

func (storage StorageImpl) Get(key string) (string, error) {
	var val, err = storage.MemTable.Find(key)
	if err == nil {
		return val, err
	}
	for _, ssTable := range *storage.SsTables {
		val, err = ssTable.Find(key)
		if val != "" {
			return val, err
		}
	}
	err = errors.New("key was not found")
	return val, err
}
