package storage

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/google/uuid"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
)

type Merger interface {
	MergeAndCompaction(ssTables []SsTable, newSsTablesCh chan<- []SsTable, errCh chan<- error)
}

type MergerImpl struct {
	MemNewFileLimit      uintptr
	StorageSstDirPath    string
	SsTableSegmentLength int64
	Mutex                sync.Mutex
}

func (m *MergerImpl) MergeAndCompaction(ssTables []SsTable, newSsTablesCh chan<- []SsTable, errCh chan<- error) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	result, err := m.MergeDescenting(ssTables)
	newSsTablesCh <- result
	errCh <- err
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
	JPath    string
	Segments []Segment
}

func (s *SSTFile) init(ssTable SsTable) {
	s.FilePath = ssTable.DPath
	s.JPath = ssTable.JPath

	s.Segments = make([]Segment, 0)

	for _, v := range ssTable.Ind {
		segment := Segment{
			First: v.start,
			Last:  v.end,
		}

		s.Segments = append(s.Segments, segment)
	}

	sort.Slice(s.Segments, func(i, j int) bool {
		return s.Segments[i].First < s.Segments[j].First
	})
}

func (m *MergerImpl) GetUnzipSegment(ssTFile SSTFile, segmentNumber int) ([]KeyValue, error) {
	result := make([]KeyValue, 0)
	var zipper Zip
	zipper = GZip{}

	file, err := os.OpenFile(ssTFile.FilePath, os.O_RDONLY, 0644)
	if err != nil {
		return result, err
	}
	defer file.Close()

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
		if len(pairElements) > 1 {
			storageKey := pairElements[0]
			storageValue := pairElements[1]
			keyValue := KeyValue{
				Value: storageValue,
				Key:   storageKey,
			}
			result = append(result, keyValue)
		}
	}

	return result, nil
}

var (
	ErrSegmentsIsOver = errors.New("segments is over")
)

func (m *MergerImpl) GetNextSeg(curFile1SegLine *int, file1Seg *[]KeyValue, curFile1Seg *int, files1 []SSTFile, curFile1 *int, files1Len *int) ([]KeyValue, error) {
	if *curFile1SegLine == len(*file1Seg) {
		if *curFile1 != *files1Len {
			if *curFile1Seg == len(files1[*curFile1].Segments) {
				*curFile1++
				*curFile1Seg = 0
			}
		} else {
			return nil, ErrSegmentsIsOver
		}

		if *curFile1 != *files1Len {
			*curFile1SegLine = 0
			*file1Seg, _ = m.GetUnzipSegment(files1[*curFile1], *curFile1Seg)
			*curFile1Seg++
		} else {
			return nil, ErrSegmentsIsOver
		}
	}

	return *file1Seg, nil
}

func (m *MergerImpl) MakeSsTable(keyValuePool []KeyValuePair) SsTable {
	avl := avltree.NewAVLTreeOrderedKey[string, string]()
	for _, item := range keyValuePool {
		avl.Insert(item.Key, item.Value)
	}

	var id = uuid.New()
	filePath := filepath.Join(m.StorageSstDirPath, id.String())
	journalPath := filepath.Join(m.StorageSstDirPath, "journal")

	err := os.MkdirAll(journalPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating ssTable journal dir. Err: %s", err)
	}

	var newTable = SsTable{DPath: filePath + ".bin", JPath: filepath.Join(journalPath, id.String()) + ".bin", SegLen: m.SsTableSegmentLength, Ind: make(map[string]SparseIndices),
		Id: uuid.New()}

	err = newTable.InitFromAvl(*avl)
	if err != nil {
		return SsTable{}
	}

	return newTable
}

func (m *MergerImpl) WriteTail(curNewFileSize uintptr, curFile1SegLine *int, curFile1Seg *int, file1SegPtr *[]KeyValue, curFile1 *int, files1 *[]SSTFile, result *[]SsTable, keyValuePool *[]KeyValuePair) {
	files1Len := len(*files1)
	file1Seg := *file1SegPtr
	for {
		var err error
		file1Seg, err = m.GetNextSeg(curFile1SegLine, &file1Seg, curFile1Seg, *files1, curFile1, &files1Len)
		if err != nil {
			if len(*keyValuePool) != 0 {
				*result = append(*result, m.MakeSsTable(*keyValuePool))
			}
			return
		}

		key := file1Seg[*curFile1SegLine].Key
		value := file1Seg[*curFile1SegLine].Value
		*curFile1SegLine++
		dataSize := unsafe.Sizeof(key) + unsafe.Sizeof(value) + 8
		if dataSize+curNewFileSize < m.MemNewFileLimit {
			curNewFileSize += dataSize
			*keyValuePool = append(*keyValuePool, KeyValuePair{Key: key, Value: value})
		} else {
			if dataSize+curNewFileSize == m.MemNewFileLimit {
				*keyValuePool = append(*keyValuePool, KeyValuePair{Key: key, Value: value})
			}
			*result = append(*result, m.MakeSsTable(*keyValuePool))
			// TODO should be replaced with clear function from go1.21 (clear(*keyValuePool))
			*keyValuePool = make([]KeyValuePair, 0)
			*keyValuePool = append(*keyValuePool, KeyValuePair{Key: key, Value: value})
			curNewFileSize = dataSize
		}
	}
}

func (m *MergerImpl) Merge(ssT1 []SsTable, ssT2 []SsTable) ([]SsTable, error) {
	var (
		result []SsTable
		files1 []SSTFile
		files2 []SSTFile
	)

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

	file1Seg, _ := m.GetUnzipSegment(files1[curFile1], curFile1Seg)
	file2Seg, _ := m.GetUnzipSegment(files2[curFile2], curFile2Seg)
	curFile1Seg++
	curFile2Seg++

	// size in bytes
	var curNewFileSize uintptr
	keyValuePool := make([]KeyValuePair, 0)
	for {
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

		dataSize := unsafe.Sizeof(key) + unsafe.Sizeof(value) + 8
		if dataSize+curNewFileSize < m.MemNewFileLimit {
			curNewFileSize += dataSize
			keyValuePool = append(keyValuePool, KeyValuePair{Key: key, Value: value})
		} else {
			if dataSize+curNewFileSize == m.MemNewFileLimit {
				keyValuePool = append(keyValuePool, KeyValuePair{Key: key, Value: value})
			}
			result = append(result, m.MakeSsTable(keyValuePool))
			keyValuePool = make([]KeyValuePair, 0)
			keyValuePool = append(keyValuePool, KeyValuePair{Key: key, Value: value})
			curNewFileSize = dataSize
		}
		var err1, err2 error
		file1Seg, err1 = m.GetNextSeg(&curFile1SegLine, &file1Seg, &curFile1Seg, files1, &curFile1, &files1Len)
		file2Seg, err2 = m.GetNextSeg(&curFile2SegLine, &file2Seg, &curFile2Seg, files2, &curFile2, &files2Len)
		if err1 != nil && err2 != nil {
			if len(keyValuePool) != 0 {
				result = append(result, m.MakeSsTable(keyValuePool))
			}
			break
		}

		if err1 != nil {
			m.WriteTail(curNewFileSize, &curFile2SegLine, &curFile2Seg, &file2Seg, &curFile2, &files2, &result, &keyValuePool)
			break
		}

		if err2 != nil {
			m.WriteTail(curNewFileSize, &curFile1SegLine, &curFile1Seg, &file1Seg, &curFile1, &files1, &result, &keyValuePool)
			break
		}
	}

	for _, file := range files1 {
		if err := os.Remove(file.FilePath); err != nil {
			return nil, err
		}
		if err := os.Remove(file.JPath); err != nil {
			return nil, err
		}
		zipFilePath := []rune(file.FilePath)
		zipFilePath = zipFilePath[0 : len(zipFilePath)-2]
		zipFilePathS := string(zipFilePath) + "bin"
		if err := os.Remove(zipFilePathS); err != nil {
			return nil, err
		}

	}

	for _, file := range files2 {
		if err := os.Remove(file.FilePath); err != nil {
			return nil, err
		}
		if err := os.Remove(file.JPath); err != nil {
			return nil, err
		}
		zipFilePath := []rune(file.FilePath)
		zipFilePath = zipFilePath[0 : len(zipFilePath)-2]
		zipFilePathS := string(zipFilePath) + "bin"
		if err := os.Remove(zipFilePathS); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (m *MergerImpl) MergeDescenting(ssTables []SsTable) ([]SsTable, error) {
	if len(ssTables) <= 1 {
		return ssTables, nil
	}

	mid := len(ssTables) / 2

	leftSsT, err := m.MergeDescenting(ssTables[:mid])
	if err != nil {
		return nil, err
	}
	rightSsT, err := m.MergeDescenting(ssTables[mid:])
	if err != nil {
		return nil, err
	}

	return m.Merge(leftSsT, rightSsT)
}
