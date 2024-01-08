package service

import (
	"PentHouseClub/internal/storage-service/config"
	"PentHouseClub/internal/storage-service/storage"
	"PentHouseClub/internal/storage-service/storage/impl"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"strconv"
)

type Sharder struct {
	Storages []storage.Storage
}

func (s Sharder) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	sectionNumber := getSectionNumber(key, len(s.Storages))
	value, getFunctionErr := s.Storages[sectionNumber].Get(r.Context(), key)

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

func (s Sharder) Set(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	respMessage := "OK"
	respError := ""

	sectionNumber := getSectionNumber(key, len(s.Storages))
	setFunctionErr := s.Storages[sectionNumber].Set(r.Context(), key, value)
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

func NewSharder(configInfo config.LSMconfig, memTables []storage.MemTable, journalPath string, ssTables map[int][]storage.SsTable, dirPath string) Sharder {
	var storages []storage.Storage
	for i := 0; i < configInfo.SectionCount; i++ {
		newDirPath := dirPath + strconv.Itoa(i)
		newJournalPath := journalPath + strconv.Itoa(i)
		err := os.MkdirAll(newDirPath, 0777)
		if err != nil {
			log.Printf("error occuring while creating ssTables dir. Err: %s", err)
		}
		err = os.MkdirAll(newJournalPath, 0777)
		if err != nil {
			log.Printf("error occuring while creating journal dir. Err: %s", err)
		}
		merger := &storage.MergerImpl{
			MemNewFileLimit:      memTables[i].MaxSize,
			StorageSstDirPath:    newDirPath,
			SsTableSegmentLength: configInfo.SSTsegLen,
		}
		storage := impl.AvlTreeImpl{
			MemTable:             memTables[i],
			SsTableSegmentLength: configInfo.SSTsegLen,
			SsTableDir:           newDirPath,
			SsTables:             ssTables[i],
			JournalPath:          newJournalPath,
			Merger:               merger,
			MergePeriodSec:       configInfo.GCperiodSec,
			IsMerged:             true,
		}

		go storage.GC()

		storages = append(storages, &storage)
	}

	s := Sharder{Storages: storages}

	return s
}

func getSectionNumber(key string, sectionCount int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % sectionCount
}
