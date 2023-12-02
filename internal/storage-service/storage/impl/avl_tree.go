package impl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"PentHouseClub/internal/storage-service/storage"
	"github.com/google/uuid"
)

// AvlTreeImpl is an implementation of storage based on AVL tree
type AvlTreeImpl struct {
	Mutex                sync.RWMutex
	MemTable             storage.MemTable
	SsTables             *[]storage.SsTable
	SsTableSegmentLength int64
	SsTableDir           string
	JournalPath          string
	Merger               storage.Merger
	MergePeriodSec       time.Duration
	IsMerged             bool
}

const minNumberOfTables = 2

func (s *AvlTreeImpl) GC() {
	ticker := time.NewTicker(s.MergePeriodSec)
	for _ = range ticker.C {
		fmt.Println("tick start")
		//s.Mutex.Lock()
		if len(*s.SsTables) >= minNumberOfTables && !s.IsMerged {

			resultCh := make(chan []storage.SsTable)
			errCh := make(chan error)
			go s.Merger.MergeAndCompaction(*s.SsTables, resultCh, errCh)
			fmt.Println("tick end")
			resultSsTables := <-resultCh
			err := <-errCh
			if err != nil {
				panic(err)
			}

			s.SsTables = &resultSsTables
			s.IsMerged = true

		}
		//s.Mutex.Unlock()
		fmt.Println("tick end")
	}
}

func (s *AvlTreeImpl) Set(_ context.Context, key string, value string) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	err := s.MemTable.Add(key, value)
	if err != nil {
		log.Printf("Copy MemTable to the ssTable")

		id := uuid.New()

		filePath := filepath.Join(s.SsTableDir, id.String())
		journalPath := filepath.Join(s.SsTableDir, "journal")

		err = os.MkdirAll(journalPath, 0777)
		if err != nil {
			log.Printf("error occuring while creating ssTable journal dir. Err: %s", err)
		}

		newTable := storage.SsTable{
			Id:     uuid.New(),
			Ind:    make(map[string]storage.SparseIndices),
			DPath:  filePath + ".bin",
			SegLen: s.SsTableSegmentLength,
			JPath:  filepath.Join(journalPath, id.String()) + ".bin",
		}

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
		s.IsMerged = false
	} else {
		now := time.Now()
		var timestamp = now.Format("2006-01-02") + "_" + now.Format("15-04-01")

		filePath := filepath.Join(s.JournalPath, timestamp)
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Open journal error. Err: %s", err)
		}
		defer file.Close()

		_, err = file.WriteString("Add key-value pair: " + key + ":" + value + ". Time: " + time.Now().String() + "\n")
		if err != nil {
			log.Printf("Write in journal error. Err: %s", err)
		}
	}

	return nil
}

var (
	ErrKeyNotFound = errors.New("key was not found")
)

func (s *AvlTreeImpl) Get(_ context.Context, key string) (string, error) {
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

	return "", ErrKeyNotFound
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
