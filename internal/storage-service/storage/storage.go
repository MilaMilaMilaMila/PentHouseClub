package storage

import (
	"errors"
	"github.com/google/uuid"
	"log"
	"os"
	"path/filepath"
	"time"
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
	JournalPath          string
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
		os.RemoveAll(storage.JournalPath)
		*storage.SsTables = append(*storage.SsTables, newTable)
	} else {
		now := time.Now()
		var timestamp = now.Format("2006-01-02") + "_" + now.Format("15-04-01")

		filePath := filepath.Join(storage.JournalPath, timestamp)
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Open journal error. Err: %s", err)
		}
		defer func() {
			if err = file.Close(); err != nil {
				log.Printf("Close journal error. Err: %s", err)
			}
		}()
		_, err = file.WriteString("Add key-value pair: " + key + ":" + value + ". Time: " + time.Now().String() + "\n")
		if err != nil {
			log.Printf("Write in journal error. Err: %s", err)
		}
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
