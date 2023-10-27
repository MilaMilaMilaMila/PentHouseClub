package storage

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"os"
)

type SegmentZip interface {
	Zip(file *os.File, sparseIndex *map[string]int64, segmentLength int64)
	Unzip(segment *bytes.Buffer) []byte
}

type SegmentGZip struct{}

func (segmentGZip SegmentGZip) Zip(file *os.File, sparseIndex *map[string]int64, segmentLength int64) {
	compressedFile, err := os.Create("tmp.gz")
	if err != nil {
		log.Printf("Zip sstable file error. Err: %s", err)
	}
	defer func(compressedFile *os.File) {
		err := compressedFile.Close()
		if err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}(compressedFile)
	writer := gzip.NewWriter(compressedFile)
	defer func(writer *gzip.Writer) {
		err := writer.Close()
		if err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}(writer)

	index := *sparseIndex
	for keyTable := range index {
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
		_, err = writer.Write(data)
		if err != nil {
			log.Printf("Zip sstable segment error. Err: %s", err)
		}
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		log.Printf("Open sstable segment error. Err: %s", err)
	}

	_, err = io.Copy(file, compressedFile)
	if err != nil {
		log.Printf("Copy zip sstable segment error. Err: %s", err)
	}
}

func (segmentGZip SegmentGZip) Unzip(segment *bytes.Buffer) []byte {
	var decompressedBuffer bytes.Buffer

	reader, err := gzip.NewReader(bytes.NewReader(segment.Bytes()))
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
