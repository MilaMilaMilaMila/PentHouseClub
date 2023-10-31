package service

import (
	"PentHouseClub/internal/storage-service/storage"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type StorageService interface {
	Get(w http.ResponseWriter, r *http.Request)
	Set(w http.ResponseWriter, r *http.Request)
}

type StorageServiceImpl struct {
	Storage storage.Storage
}

func (storageService StorageServiceImpl) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value_channel := make(chan string)
	getFunctionErr_channel := make(chan error)
	go storageService.Storage.Get(key, value_channel, getFunctionErr_channel)
	value, getFunctionErr := <-value_channel, <-getFunctionErr_channel

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

func (storageService StorageServiceImpl) Set(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	respMessage := "OK"
	respError := ""
	setFunctionErr_channel := make(chan error)
	go storageService.Storage.Set(key, value, setFunctionErr_channel)
	setFunctionErr := <-setFunctionErr_channel
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
