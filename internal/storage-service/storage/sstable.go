package storage

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
)

type Zip interface {
	Zip(dirPath string, sparseIndex *map[string]SparseIndices, segmentLength int64) (string, map[string]SparseIndices, int64)
	Unzip(segment *[]byte) []byte
}

type SparseIndices struct {
	start int64
	end   int64
}

type SsTable struct {
	DPath  string
	JPath  string
	SegLen int64
	Ind    map[string]SparseIndices
	Id     uuid.UUID
}

func (table *SsTable) InitFromAvl(mt avltree.AVLTree[string, string]) error {
	var currSize int64
	var segCount int64
	file, err := os.OpenFile(table.DPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	mt.Enumerate(avltree.ASCENDING, func(key string, value string) bool {
		line := ";" + key + ":" + value
		if isFirst {
			line = key + ":" + value
			isFirst = false
		}
		data := []byte(line)
		dataSize := (int64)(len(data))
		if dataSize > table.SegLen {
			err = errors.New("segments of SSTable are too small to fit the key-value")
			return false
		}
		if currSize+dataSize > table.SegLen {
			currSize = 0
			segCount += 1
		}
		if currSize == 0 {
			table.Ind[key] = SparseIndices{segCount * table.SegLen, segCount*table.SegLen + table.SegLen}
		}
		bytesCount, writeError := file.Write(data)
		if writeError != nil {
			log.Printf("Write data in sstable file error. Err: %s", writeError)
			err = writeError
			return false
		}
		currSize += (int64)(bytesCount)
		return true
	})
	var zipper Zip
	zipper = GZip{}
	table.DPath, table.Ind, table.SegLen = zipper.Zip(table.DPath, &table.Ind, table.SegLen)

	journal, err := os.OpenFile(table.JPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Open journal error. Err: %s", err)
	}
	defer func() {
		if err = journal.Close(); err != nil {
			log.Printf("Close journal error. Err: %s", err)
		}
	}()
	for keyTable := range table.Ind {
		start := table.Ind[keyTable].start
		end := table.Ind[keyTable].end
		_, err = journal.WriteString(keyTable + ":" + strconv.FormatInt(start, 10) + ":" + strconv.FormatInt(end, 10) + "\n")
		if err != nil {
			log.Printf("Write in journal error. Err: %s", err)
		}
	}

	return err
}

func (table *SsTable) Init(mt MemTable) error {
	var currSize int64
	var segCount int64
	file, err := os.OpenFile(table.DPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	mt.AvlTree.Enumerate(avltree.ASCENDING, func(key string, value string) bool {
		line := ";" + key + ":" + value
		if isFirst {
			line = key + ":" + value
			isFirst = false
		}
		data := []byte(line)
		dataSize := (int64)(len(data))
		if dataSize > table.SegLen {
			err = errors.New("segments of SSTable are too small to fit the key-value")
			return false
		}
		if currSize+dataSize > table.SegLen {
			currSize = 0
			segCount += 1
		}
		if currSize == 0 {
			table.Ind[key] = SparseIndices{segCount * table.SegLen, segCount*table.SegLen + table.SegLen}
		}
		bytesCount, writeError := file.Write(data)
		if writeError != nil {
			log.Printf("Write data in sstable file error. Err: %s", writeError)
			err = writeError
			return false
		}
		currSize += (int64)(bytesCount)
		return true
	})
	var zipper Zip
	zipper = GZip{}
	table.DPath, table.Ind, table.SegLen = zipper.Zip(table.DPath, &table.Ind, table.SegLen)

	journal, err := os.OpenFile(table.JPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Open journal error. Err: %s", err)
	}
	defer func() {
		if err = journal.Close(); err != nil {
			log.Printf("Close journal error. Err: %s", err)
		}
	}()
	for keyTable := range table.Ind {
		start := table.Ind[keyTable].start
		end := table.Ind[keyTable].end
		_, err = journal.WriteString(keyTable + ":" + strconv.FormatInt(start, 10) + ":" + strconv.FormatInt(end, 10) + "\n")
		if err != nil {
			log.Printf("Write in journal error. Err: %s", err)
		}
	}

	return err
}

type KeyValuePair struct {
	Key   string
	Value string
}

func (table *SsTable) InitFromSlice(keyValue []KeyValuePair) error {
	var currentSize int64
	var segmentsCount int64
	file, err := os.OpenFile(table.DPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	WriteInFile := func(key string, value string) {
		line := ";" + key + ":" + value
		if isFirst {
			line = key + ":" + value
			isFirst = false
		}
		data := []byte(line)
		dataSize := (int64)(len(data))
		if dataSize > table.SegLen {
			err = errors.New("segments of SSTable are too small to fit the key-value")
			return
		}
		if currentSize+dataSize > table.SegLen {
			currentSize = 0
			segmentsCount += 1
		}
		if currentSize == 0 {
			table.Ind[key] = SparseIndices{segmentsCount * table.SegLen, segmentsCount*table.SegLen + table.SegLen}
		}
		bytesCount, writeError := file.Write(data)
		if writeError != nil {
			log.Printf("Write data in sstable file error. Err: %s", writeError)
			err = writeError
			return
		}
		currentSize += (int64)(bytesCount)
		return
	}

	for _, i := range keyValue {
		WriteInFile(i.Key, i.Value)
	}

	var zipper Zip
	zipper = GZip{}
	table.DPath, table.Ind, table.SegLen = zipper.Zip(table.DPath, &table.Ind, table.SegLen)

	journal, err := os.OpenFile(table.JPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Open journal error. Err: %s", err)
	}
	defer func() {
		if err = journal.Close(); err != nil {
			log.Printf("Close journal error. Err: %s", err)
		}
	}()
	for keyTable := range table.Ind {
		start := table.Ind[keyTable].start
		end := table.Ind[keyTable].end
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
	for keyTable := range table.Ind {
		if key < keyTable {
			continue
		} else {
			if maxIndex <= table.Ind[keyTable].start {
				maxIndex = table.Ind[keyTable].start
				neededKey = keyTable
			}
			flagLine = true
		}
	}
	neededSegmentLine = maxIndex
	if !flagLine {
		keyLineError = errors.New(fmt.Sprintf("key %s was not found", key))
		log.Printf("SsTable with id %s does not contain key", table.Id)
		return "", keyLineError
	}
	var zipper Zip
	zipper = GZip{}
	file, err := os.OpenFile(table.DPath, os.O_RDONLY, 0644)
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

	data := make([]byte, table.Ind[neededKey].end-table.Ind[neededKey].start)
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
		if len(pairElements) > 1 {
			storageKey := pairElements[0]
			storageValue := pairElements[1]
			if storageKey == key {
				value = storageValue
				return value, nil
			}
		}
	}
	log.Printf("In the ssTable with id %s key was not found", table.Id.String())
	return value, nil
}

func (table *SsTable) BuildSparseIndex() {
	journal, err := os.OpenFile(table.JPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("Open ssTable journal with id %s error", table.Id.String())
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

	table.Ind = index
}

func Restore(dirPath string, journalPath string, journalName string) SsTable {
	idLen := len(journalName) - 4
	zipPath := dirPath[:len(dirPath)-4] + ".gz"
	ssTable := SsTable{DPath: zipPath, JPath: journalPath, Id: uuid.MustParse(journalName[:idLen]), Ind: make(map[string]SparseIndices)}
	ssTable.BuildSparseIndex()
	return ssTable
}
