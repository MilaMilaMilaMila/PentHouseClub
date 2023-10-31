package storage

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Merger interface {
	MergeAndCompaction(ssTables []SsTable, newSsTables chan<- []SsTable)
}

type MergerImpl struct {
	MemNewFileLimit      uintptr
	StorageSstDirPath    string
	SsTableSegmentLength int64
	Mutex                sync.Mutex
}

func (merger MergerImpl) MergeAndCompaction(ssTables []SsTable, newSsTables chan<- []SsTable) {
	merger.Mutex.Lock()
	defer merger.Mutex.Unlock()
	if len(ssTables) < 2 {
		newSsTables <- ssTables
		return
	}
	result := merger.MergeDescenting(ssTables)
	newSsTables <- result
	return
}

type KeyValue struct {
	Key   string
	Value string
}

type Segment struct {
	First int64
	Last  int64
}

type SSTFile struct {
	FilePath string
	Segments []Segment
}

func (sstFile *SSTFile) init(ssTable SsTable) {
	sstFile.FilePath = ssTable.dPath
	sstFile.Segments = make([]Segment, 0)
	for _, v := range ssTable.ind {
		segment := Segment{
			First: v.start,
			Last:  v.end,
		}
		sstFile.Segments = append(sstFile.Segments, segment)
	}

	sort.Slice(sstFile.Segments, func(i, j int) bool {
		return sstFile.Segments[i].First < sstFile.Segments[j].First
	})
}

func (merger MergerImpl) GetUnzipSegment(ssTFile SSTFile, segmentNumber int) ([]KeyValue, error) {
	result := make([]KeyValue, 0)
	var zipper Zip
	zipper = GZip{}
	file, err := os.OpenFile(ssTFile.FilePath, os.O_RDONLY, 0644)
	if err != nil {
		return result, err
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}()
	_, err = file.Seek(ssTFile.Segments[segmentNumber].First, 0)
	if err != nil {
		return result, nil
	}

	data := make([]byte, ssTFile.Segments[segmentNumber].Last-ssTFile.Segments[segmentNumber].First)
	n, err := file.Read(data)
	if err != nil {
		return result, nil
	}
	decompressedData := zipper.Unzip(&data)
	segment := string(decompressedData[:n])
	keyValuePairs := strings.Split(segment, ";")
	for _, kvp := range keyValuePairs {
		pairElements := strings.Split(kvp, ":")
		storageKey := pairElements[0]
		storageValue := pairElements[1]
		keyValue := KeyValue{
			Value: storageValue,
			Key:   storageKey,
		}
		result = append(result, keyValue)
	}

	return result, nil
}

func (merger MergerImpl) GetNextSeg(curFile1SegLine *int, file1Seg *[]KeyValue, curFile1Seg *int, files1 []SSTFile, curFile1 *int, files1Len *int) ([]KeyValue, error) {
	var err error
	if *curFile1SegLine == len(*file1Seg) {
		if *curFile1 != *files1Len {
			if *curFile1Seg == len(files1[*curFile1].Segments) {
				*curFile1++
				*curFile1Seg = 0
			}
		} else {
			err = os.ErrNotExist
			return *file1Seg, err
		}

		if *curFile1 != *files1Len {
			*curFile1SegLine = 0
			*file1Seg, _ = merger.GetUnzipSegment(files1[*curFile1], *curFile1Seg)
			*curFile1Seg++
			err = nil
		} else {
			err = os.ErrNotExist
		}
	} else {
		err = nil
	}

	return *file1Seg, err
}

func (merger MergerImpl) MakeSsTable(keyValuePool []KeyValuePair) SsTable {

	var id = uuid.New()
	filePath := filepath.Join(merger.StorageSstDirPath, id.String())
	journalPath := filepath.Join(merger.StorageSstDirPath, "journal")
	err := os.Mkdir(journalPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating ssTable journal dir. Err: %s", err)
	}
	var newTable = SsTable{dPath: filePath + ".bin", jPath: filepath.Join(journalPath, id.String()) + ".bin", segLen: merger.SsTableSegmentLength, ind: make(map[string]SparseIndices),
		id: uuid.New()}
	err = newTable.InitFromSlice(keyValuePool)
	if err != nil {
		return SsTable{}
	}
	return newTable

}

func (merger MergerImpl) WriteTail(curFile1SegLine *int, curFile1Seg *int, file1SegPtr *[]KeyValue, curFile1 *int, files1 *[]SSTFile, result *[]SsTable, keyValuePool *[]KeyValuePair) {
	isFirst := true
	files1Len := len(*files1)
	var curNewFileSize uintptr
	file1Seg := *file1SegPtr
	for true {
		var err error
		file1Seg, err = merger.GetNextSeg(curFile1SegLine, &file1Seg, curFile1Seg, *files1, curFile1, &files1Len)
		if err != nil {
			return
		}

		key := file1Seg[*curFile1SegLine].Key
		value := file1Seg[*curFile1SegLine].Value
		*curFile1SegLine++
		line := ";" + key + ":" + value
		if isFirst {
			line = key + ":" + value
			isFirst = false
		}
		data := []byte(line)
		dataSize := (uintptr)(len(data))
		if dataSize+curNewFileSize <= merger.MemNewFileLimit {
			curNewFileSize += dataSize
			*keyValuePool = append(*keyValuePool, KeyValuePair{Key: key, Value: value})
		} else {
			isFirst = true
			curNewFileSize = 0
			*result = append(*result, merger.MakeSsTable(*keyValuePool))
			*keyValuePool = make([]KeyValuePair, 0)
			*keyValuePool = append(*keyValuePool, KeyValuePair{Key: key, Value: value})
			curNewFileSize = dataSize
		}
	}
}

func (merger MergerImpl) Merge(ssT1 []SsTable, ssT2 []SsTable) []SsTable {
	result := make([]SsTable, 0)
	files1 := make([]SSTFile, 0)
	files2 := make([]SSTFile, 0)
	for _, sst := range ssT1 {
		ssTFile := SSTFile{}
		ssTFile.init(sst)
		files1 = append(files1, ssTFile)
	}
	for _, sst := range ssT2 {
		ssTFile := SSTFile{}
		ssTFile.init(sst)
		files2 = append(files2, ssTFile)
	}
	curFile1, curFile2 := 0, 0
	curFile1Seg, curFile2Seg := 0, 0
	curFile1SegLine, curFile2SegLine := 0, 0

	files1Len, files2Len := len(files1), len(files2)

	file1Seg, _ := merger.GetUnzipSegment(files1[curFile1], curFile1Seg)
	file2Seg, _ := merger.GetUnzipSegment(files2[curFile2], curFile2Seg)
	curFile1Seg++
	curFile2Seg++

	// size in bytes
	var curNewFileSize uintptr
	isFirst := true
	keyValuePool := make([]KeyValuePair, 0)
	for true {
		var key, value string
		if file1Seg[curFile1SegLine].Key == file2Seg[curFile2SegLine].Key {
			key = file2Seg[curFile2SegLine].Key
			value = file2Seg[curFile2SegLine].Value
			curFile1SegLine++
			curFile2SegLine++
		} else if file1Seg[curFile1SegLine].Key < file2Seg[curFile2SegLine].Key {
			key = file1Seg[curFile1SegLine].Key
			value = file1Seg[curFile1SegLine].Value
			curFile1SegLine++
		} else if file1Seg[curFile1SegLine].Key > file2Seg[curFile2SegLine].Key {
			key = file2Seg[curFile2SegLine].Key
			value = file2Seg[curFile2SegLine].Value
			curFile2SegLine++
		}

		line := ";" + key + ":" + value
		if isFirst {
			line = key + ":" + value
			isFirst = false
		}
		data := []byte(line)
		dataSize := (uintptr)(len(data))
		if dataSize+curNewFileSize <= merger.MemNewFileLimit {
			curNewFileSize += dataSize
			keyValuePool = append(keyValuePool, KeyValuePair{Key: key, Value: value})
		} else {
			isFirst = true
			curNewFileSize = 0
			result = append(result, merger.MakeSsTable(keyValuePool))
			keyValuePool = make([]KeyValuePair, 0)
			keyValuePool = append(keyValuePool, KeyValuePair{Key: key, Value: value})
			curNewFileSize = dataSize
		}
		var err error
		file1Seg, err = merger.GetNextSeg(&curFile1SegLine, &file1Seg, &curFile1Seg, files1, &curFile1, &files1Len)
		if err != nil {
			merger.WriteTail(&curFile1SegLine, &curFile1Seg, &file1Seg, &curFile1, &files1, &result, &keyValuePool)
			break
		}
		file2Seg, err = merger.GetNextSeg(&curFile2SegLine, &file2Seg, &curFile2Seg, files2, &curFile2, &files2Len)
		if err != nil {
			merger.WriteTail(&curFile2SegLine, &curFile2Seg, &file2Seg, &curFile2, &files2, &result, &keyValuePool)
			break
		}
	}

	return result
}

func (merger MergerImpl) MergeDescenting(ssTables []SsTable) []SsTable {
	if len(ssTables) <= 1 {
		return ssTables
	}
	mid := len(ssTables) / 2
	fmt.Println(mid)
	leftSsT := merger.MergeDescenting(ssTables[:mid])
	rightSsT := merger.MergeDescenting(ssTables[mid:])

	return merger.Merge(leftSsT, rightSsT)
}
