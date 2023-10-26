package storage

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"unsafe"
)

type ssTable struct {
	dirPath       string
	segmentLength uintptr
	sparseIndex   map[int64]string
}

func (table ssTable) Add(memTable MemTable) error {
	currentSize := uintptr(0)
	lines := int64(0)
	for i := range memTable.AvlTree.Iter() {
		file, err := os.OpenFile(table.dirPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer func() {
			if err = file.Close(); err != nil {
				log.Printf("Close sstable file error. Err: %s", err)
			}
		}()
		line := i.Key + ":" + i.Value.(string) + "\n"
		if unsafe.Sizeof(line) > table.segmentLength {
			return errors.New("segments of SSTable are too small to fit the key-value")
		}
		if currentSize+unsafe.Sizeof(line) > table.segmentLength {
			currentSize = 0
		}
		if currentSize == 0 {
			table.sparseIndex[lines] = i.Key
		}
		currentSize += unsafe.Sizeof(line)
		lines += 1
		_, writeError := file.WriteString(line)
		if writeError != nil {
			log.Printf("Write data in sstable file error. Err: %s", writeError)
			return writeError
		}
	}
	return nil
}

func (table ssTable) Find(key string) (string, error) {
	flagLine := false
	var neededSegmentLine int64
	var keyLineError error
	for keyTable := range table.sparseIndex {
		if key < table.sparseIndex[keyTable] {
			continue
		} else {
			neededSegmentLine = keyTable
			flagLine = true
		}
	}
	if !flagLine {
		keyLineError = errors.New(fmt.Sprintf("Key %s does not exist", key))
		log.Printf("Not existing key error. Err: %s", keyLineError)
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

	flag := false
	value := ""
	var keyError error

	counter := int64(0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if counter == neededSegmentLine {
			line := scanner.Text()
			lineElements := strings.Split(line, ":")
			if len(line) == 0 {
				keyError = errors.New(fmt.Sprintf("Key %s does not exist", key))
				log.Printf("Not existing key error. Err: %s", keyError)
				return value, keyError
			}
			storageKey := lineElements[0]
			storageValue := lineElements[1]

			if storageKey == key {
				value = storageValue
				flag = true
				return value, nil
			}
			break
		}
		counter += 1
	}

	if !flag {
		keyError = errors.New(fmt.Sprintf("Key %s does not exist", key))
		log.Printf("Not existing key error. Err: %s", keyError)
	}

	if scannerErr := scanner.Err(); err != nil {
		log.Printf("Scanner error. Err: %s", scannerErr)
		return "", scannerErr
	}

	return "", err
}
