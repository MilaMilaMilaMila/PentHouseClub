package service

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type Storage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	GC()
}

type StorageServiceImpl struct {
	Storage Storage
}

// TODO нужен конструктор
func (s StorageServiceImpl) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, getFunctionErr := s.Storage.Get(key)

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
	setFunctionErr := s.Storage.Set(key, value)
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
