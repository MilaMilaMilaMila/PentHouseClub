package storage

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
)

type Zip interface {
	Zip(dirPath string, sparseIndex *map[string]SparseIndices, segmentLength int64) (string, map[string]SparseIndices, int64)
	Unzip(segment *[]byte) []byte
}

type GZip struct{}

func (z GZip) Zip(dirPath string, sparseIndex *map[string]SparseIndices, segmentLength int64) (string, map[string]SparseIndices, int64) {
	file, err := os.OpenFile(dirPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("Open sstable file error. Err: %s", err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}()

	ndp := dirPath[:len(dirPath)-4] + ".gz"
	cf, err := os.OpenFile(ndp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Zip sstable file error. Err: %s", err)
	}
	defer func(compressedFile *os.File) {
		err := compressedFile.Close()
		if err != nil {
			log.Printf("Close sstable file error. Err: %s", err)
		}
	}(cf)

	i := *sparseIndex
	newI := make(map[string]SparseIndices)
	newSeg := int64(0)
	newSegLen := int64(0)
	f := false
	for keyTable := range i {
		sedLen := i[keyTable].start
		_, err = file.Seek(sedLen, 0)
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}
		data := make([]byte, segmentLength)
		_, err := file.Read(data)
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}

		var cBuff bytes.Buffer
		buffWriter := gzip.NewWriter(&cBuff)
		_, err = buffWriter.Write(data)
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}
		err = buffWriter.Close()
		if err != nil {
			log.Printf("Segment sstable file error. Err: %s", err)
		}

		currentPosition, err := cf.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Println(err)
		}

		n2, err := cf.Write(cBuff.Bytes())
		if err != nil {
			log.Printf("Zip sstable segment error. Err: %s", err)
		}
		newI[keyTable] = SparseIndices{newSeg, currentPosition + int64(n2)}
		newSeg += int64(n2)

		if f == false {
			newSegLen = int64(n2)
			f = true
		}
		if err != nil {
			log.Printf("Index zip sstable segment error. Err: %s", err)
		}
	}

	return ndp, newI, newSegLen
}

func (z GZip) Unzip(segment *[]byte) []byte {
	var decompBuff bytes.Buffer

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

	_, err = io.Copy(&decompBuff, reader)
	if err != nil {
		log.Printf("Unzip sstable segment error. Err: %s", err)
	}

	return decompBuff.Bytes()
}
