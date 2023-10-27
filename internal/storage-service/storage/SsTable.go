package storage

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
	"log"
	"os"
	"strings"
)

type SsTable struct {
	dirPath       string
	segmentLength int64
	sparseIndex   map[string]int64
	id            uuid.UUID
}

func (table SsTable) Init(memTable MemTable) error {
	var currentSize int64
	var segmentsCount int64
	file, err := os.OpenFile(table.dirPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}()
	err = nil
	isFirst := true
	memTable.AvlTree.Enumerate(avltree.ASCENDING, func(key string, value string) bool {
		line := ";" + key + ":" + value
		if isFirst {
			line = key + ":" + value
			isFirst = false
		}
		data := []byte(line)
		dataSize := (int64)(len(data))
		if dataSize > table.segmentLength {
			err = errors.New("segments of SSTable are too small to fit the key-value")
			return false
		}
		if currentSize+dataSize > table.segmentLength {
			currentSize = 0
			segmentsCount += 1
		}
		if currentSize == 0 {
			table.sparseIndex[key] = segmentsCount * table.segmentLength
		}
		bytesCount, writeError := file.Write(data)
		if writeError != nil {
			log.Printf("Write data in sstable file error. Err: %s", writeError)
			err = writeError
			return false
		}
		currentSize += (int64)(bytesCount)
		return true
	})
	return err
}

func (table SsTable) Find(key string) (string, error) {
	flagLine := false
	var neededSegmentLine int64
	var keyLineError error
	for keyTable := range table.sparseIndex {
		if key < keyTable {
			continue
		} else {
			neededSegmentLine = table.sparseIndex[keyTable]
			flagLine = true
		}
	}
	if !flagLine {
		keyLineError = errors.New(fmt.Sprintf("key %s was not found", key))
		log.Printf("SsTable with id %s does not contain key", table.id)
		return "", keyLineError
	}
	file, err := os.OpenFile(table.dirPath, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}()
	_, err = file.Seek(neededSegmentLine, 0)
	if err != nil {
		return "", nil
	}
	data := make([]byte, table.segmentLength)
	n, err := file.Read(data)
	if err != nil {
		return "", nil
	}
	segment := string(data[:n])
	value := ""
	keyValuePairs := strings.Split(segment, ";")
	for _, kvp := range keyValuePairs {
		pairElements := strings.Split(kvp, ":")
		storageKey := pairElements[0]
		storageValue := pairElements[1]
		if storageKey == key {
			value = storageValue
			return value, nil
		}
	}
	log.Printf("In the ssTable with id %s key was not found", table.id.String())
	return value, nil
}
