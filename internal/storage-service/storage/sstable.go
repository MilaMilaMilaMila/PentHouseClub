package storage

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
	"log"
	"os"
	"strconv"
	"strings"
)

type SparseIndices struct {
	start int64
	end   int64
}

type SsTable struct {
	dirPath       string
	journalPath   string
	segmentLength int64
	sparseIndex   map[string]SparseIndices
	id            uuid.UUID
}

func (table *SsTable) Init(memTable MemTable) error {
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
			table.sparseIndex[key] = SparseIndices{segmentsCount * table.segmentLength, segmentsCount*table.segmentLength + table.segmentLength}
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
	var zipper SegmentZip
	zipper = SegmentGZip{}
	table.dirPath, table.sparseIndex, table.segmentLength = zipper.Zip(table.dirPath, &table.sparseIndex, table.segmentLength)

	journal, err := os.OpenFile(table.journalPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Open journal error. Err: %s", err)
	}
	defer func() {
		if err = journal.Close(); err != nil {
			log.Printf("Close journal error. Err: %s", err)
		}
	}()
	for keyTable := range table.sparseIndex {
		start := table.sparseIndex[keyTable].start
		end := table.sparseIndex[keyTable].end
		_, err = journal.WriteString(keyTable + ":" + strconv.FormatInt(start, 10) + ":" + strconv.FormatInt(end, 10) + "\n")
		if err != nil {
			log.Printf("Write in journal error. Err: %s", err)
		}
	}

	return err
}

func (table *SsTable) Find(key string) (string, error) {
	flagLine := false
	var neededSegmentLine int64
	var keyLineError error
	maxIndex := int64(0)
	neededKey := ""
	for keyTable := range table.sparseIndex {
		if key < keyTable {
			continue
		} else {
			if maxIndex <= table.sparseIndex[keyTable].start {
				maxIndex = table.sparseIndex[keyTable].start
				neededKey = keyTable
			}
			flagLine = true
		}
	}
	neededSegmentLine = maxIndex
	if !flagLine {
		keyLineError = errors.New(fmt.Sprintf("key %s was not found", key))
		log.Printf("SsTable with id %s does not contain key", table.id)
		return "", keyLineError
	}
	var zipper SegmentZip
	zipper = SegmentGZip{}
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

	data := make([]byte, table.sparseIndex[neededKey].end-table.sparseIndex[neededKey].start)
	n, err := file.Read(data)
	if err != nil {
		return "", nil
	}
	decompressedData := zipper.Unzip(&data)
	segment := string(decompressedData[:n])
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

func (table *SsTable) BuildSparseIndex() {
	journal, err := os.OpenFile(table.journalPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("Open ssTable journal with id %s error", table.id.String())
	}
	defer func() {
		if err = journal.Close(); err != nil {
			log.Printf("Close sstable journal file error. Err: %s", err)
		}
	}()

	index := make(map[string]SparseIndices)
	scanner := bufio.NewScanner(journal)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			sparseMap := strings.Split(line, ":")
			start, err := strconv.ParseInt(sparseMap[1], 10, 64)
			if err != nil {
				fmt.Println("Error converting start index from string to int64:", err)
				return
			}
			end, err := strconv.ParseInt(sparseMap[2], 10, 64)
			if err != nil {
				fmt.Println("Error converting end index from string to int64:", err)
				return
			}
			index[sparseMap[0]] = SparseIndices{start, end}
		} else {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Read sstable journal file error. Err: %s", err)
	}

	table.sparseIndex = index
}
