package service

import (
	"PentHouseClub/internal/storage-service/config"
	"PentHouseClub/internal/storage-service/storage"
	"PentHouseClub/internal/storage-service/storage/impl"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

type StorageServiceImpl struct {
	Storage storage.Storage
}

func (s StorageServiceImpl) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, getFunctionErr := s.Storage.Get(r.Context(), key)

	respMessage := "OK"
	respError := ""

	if getFunctionErr != nil {
		respMessage = "FAILED"
		respError = fmt.Sprintf("Get function error. Err: %s", getFunctionErr)
		log.Printf("Get function error. Err: %s\n", getFunctionErr)
	}

	resp := make(map[string]string)

	resp["value"] = value
	resp["message"] = respMessage
	resp["error"] = respError

	jsonResp, parseJsonErr := json.Marshal(resp)
	if parseJsonErr != nil {
		log.Printf("Error happened in JSON marshal. Err: %s", parseJsonErr)
	}

	if _, writeResponseErr := w.Write(jsonResp); writeResponseErr != nil {
		log.Printf("Write response error. Err: %s", writeResponseErr)
	}

	return
}

func (s StorageServiceImpl) Set(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	respMessage := "OK"
	respError := ""

	setFunctionErr := s.Storage.Set(r.Context(), key, value)
	if setFunctionErr != nil {
		respMessage = "FAILED"
		respError = fmt.Sprintf("Set function error. Err: %s", setFunctionErr)
		log.Printf("Set function error. Err: %s", setFunctionErr)
	}

	resp := make(map[string]string)
	resp["status"] = respMessage
	resp["error"] = respError

	jsonResp, parseJsonErr := json.Marshal(resp)
	if parseJsonErr != nil {
		log.Printf("Error happened in JSON marshal. Err: %s", parseJsonErr)
	}

	if _, writeResponseErr := w.Write(jsonResp); writeResponseErr != nil {
		log.Printf("Write response error. Err: %s", writeResponseErr)
	}

	return
}

func NewStorageServiceImpl(configInfo config.LSMconfig, memTable storage.MemTable, journalPath string, ssTables []storage.SsTable, dirPath string) StorageServiceImpl {
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating ssTables dir. Err: %s", err)
	}
	err = os.MkdirAll(journalPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating journal dir. Err: %s", err)
	}
	merger := &storage.MergerImpl{
		MemNewFileLimit:      memTable.MaxSize,
		StorageSstDirPath:    dirPath,
		SsTableSegmentLength: configInfo.SSTsegLen,
	}
	storage := impl.AvlTreeImpl{
		MemTable:             memTable,
		SsTableSegmentLength: configInfo.SSTsegLen,
		SsTableDir:           dirPath,
		SsTables:             ssTables,
		JournalPath:          journalPath,
		Merger:               merger,
		MergePeriodSec:       configInfo.GCperiodSec,
		IsMerged:             true,
	}

	go storage.GC()

	s := StorageServiceImpl{Storage: &storage}

	return s
}
