package storage

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"os"
)

type SegmentZip interface {
	Zip(dirPath string, sparseIndex *map[string]int64, segmentLength int64) (string, map[string]int64, int64)
	Unzip(segment *[]byte) []byte
}

type SegmentGZip struct{}

func (segmentGZip SegmentGZip) Zip(dirPath string, sparseIndex *map[string]int64, segmentLength int64) (string, map[string]int64, int64) {
	file, err := os.OpenFile(dirPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("Open sstable file error. Err: %s", err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}()

	newDirPath := dirPath[:len(dirPath)-4] + ".gz"
	compressedFile, err := os.OpenFile(newDirPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Zip sstable file error. Err: %s", err)
	}
	defer func(compressedFile *os.File) {
		err := compressedFile.Close()
		if err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}(compressedFile)

	index := *sparseIndex
	newIndex := make(map[string]int64)
	newSegment := int64(0)
	newSegmentLength := int64(0)
	flag := false
	for keyTable := range index {
		newIndex[keyTable] = newSegment
		neededSegmentLine := index[keyTable]
		_, err = file.Seek(neededSegmentLine, 0)
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}
		data := make([]byte, segmentLength)
		_, err := file.Read(data)
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}

		var compressedBuffer bytes.Buffer
		buffWriter := gzip.NewWriter(&compressedBuffer)
		_, err = buffWriter.Write(data)
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}
		err = buffWriter.Close()
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}

		_, err = compressedFile.Write(compressedBuffer.Bytes())
		if err != nil {
			log.Printf("Zip sstable segment error. Err: %s", err)
		}
		newSegment, err = file.Seek(0, io.SeekCurrent)
		if flag == false {
			newSegmentLength = newSegment
			flag = true
		}
		if err != nil {
			log.Printf("Index zip sstable segment error. Err: %s", err)
		}
	}

	return newDirPath, newIndex, newSegmentLength
}

func (segmentGZip SegmentGZip) Unzip(segment *[]byte) []byte {
	var decompressedBuffer bytes.Buffer

	reader, err := gzip.NewReader(io.NopCloser(bytes.NewBuffer(*segment)))
	if err != nil {
		log.Printf("Read sstable segment error. Err: %s", err)
	}
	defer func(reader *gzip.Reader) {
		err := reader.Close()
		if err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}(reader)

	_, err = io.Copy(&decompressedBuffer, reader)
	if err != nil {
		log.Printf("Unzip sstable segment error. Err: %s", err)
	}

	return decompressedBuffer.Bytes()
}
