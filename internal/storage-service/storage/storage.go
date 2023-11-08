package storage

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Storage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	GC()
}

type StorageImpl struct {
	Mutex                sync.RWMutex
	MemTable             MemTable
	SsTables             *[]SsTable
	SsTableSegmentLength int64
	SsTableDir           string
	JournalPath          string
	Merger               Merger
	MergePeriodSec       time.Duration
}

// TODO logger сделать
func (s *StorageImpl) GC() {
	ticker := time.NewTicker(s.MergePeriodSec)
	for _ = range ticker.C {
		fmt.Println("tick start")
		resultCh := make(chan []SsTable)
		errCh := make(chan error)

		go s.Merger.MergeAndCompaction(*s.SsTables, resultCh, errCh)

		resultSsTables := <-resultCh
		err := <-errCh
		if err != nil {
			panic(err)
		}

		s.SsTables = &resultSsTables
		fmt.Println("tick end")
	}
}

func (s *StorageImpl) Set(key string, value string) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	err := s.MemTable.Add(key, value)
	if err != nil {
		log.Printf("Copy MemTable to the ssTable")
		var id = uuid.New()
		filePath := filepath.Join(s.SsTableDir, id.String())
		journalPath := filepath.Join(s.SsTableDir, "journal")
		err := os.MkdirAll(journalPath, 0777)
		if err != nil {
			log.Printf("error occuring while creating ssTable journal dir. Err: %s", err)
		}
		var newTable = SsTable{dPath: filePath + ".bin", jPath: filepath.Join(journalPath, id.String()) + ".bin", segLen: s.SsTableSegmentLength, ind: make(map[string]SparseIndices),
			id: uuid.New()}
		newTable.Init(s.MemTable)
		s.MemTable.Clear()
		filenames := GetFileNamesInDir(s.JournalPath)
		if len(filenames) != 0 {
			err = os.Remove(filepath.Join(s.JournalPath, filenames[0]))
		}
		if err != nil {
			log.Printf("error occuring while deleting journal. Err: %s", err)
		}
		*s.SsTables = append(*s.SsTables, newTable)
	} else {
		now := time.Now()
		var timestamp = now.Format("2006-01-02") + "_" + now.Format("15-04-01")

		filePath := filepath.Join(s.JournalPath, timestamp)
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

func (s *StorageImpl) Get(key string) (string, error) {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	var val, err = s.MemTable.Find(key)
	if err == nil {
		return val, err
	}
	for i := len(*s.SsTables) - 1; i >= 0; i-- {
		ssTable := (*s.SsTables)[i]
		val, err = ssTable.Find(key)
		if val != "" {
			return val, err
		}
	}
	err = errors.New("key was not found")
	return "", err
}

func GetFileNamesInDir(name string) []string {
	f, err := os.Open(name)
	if err != nil {
		log.Printf("Open journal dir error. Err: %s", err)
		return make([]string, 0)
	}
	defer f.Close()
	// TODO обработать ошибачккю
	files, _ := os.ReadDir(name)
	fileNames, err := f.Readdirnames(len(files))
	if err == io.EOF {
		return make([]string, 0)
	}
	return fileNames
}
