package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

const serverPort = "8080"

type Storage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}

type StorageImpl struct {
}

func (storage StorageImpl) Set(key string, value string) error {
	file, err := os.OpenFile("cmd/storage-service/data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatalf("Close file error. Err: %s", err)
		}
	}()

	line := "key=" + key + " " + "value=" + value + "\n"
	_, writeError := file.WriteString(line)
	if writeError != nil {
		log.Fatalf("Write data in file error. Err: %s", writeError)
		return writeError
	}
	return nil
}

func (storage StorageImpl) Get(key string) (string, error) {
	file, err := os.OpenFile("cmd/storage-service/data.txt", os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatalf("Close file error. Err: %s", err)
		}
	}()

	flag := false
	value := ""
	var keyError error

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lineElements := strings.Split(line, " ")
		storageKey := strings.Split(lineElements[0], "=")[1]
		storageValue := strings.Split(lineElements[1], "=")[1]

		if storageKey == key {
			value = storageValue
			flag = true
		}
	}

	if !flag {
		keyError = os.ErrNotExist
		log.Fatalf("Not existing key error. Err: %s", keyError)
	} else {
		keyError = nil
	}

	if scannerErr := scanner.Err(); err != nil {
		log.Fatalf("Scanner error. Err: %s", scannerErr)
		return "", scannerErr
	}

	return value, keyError
}

type StorageService interface {
	Get(w http.ResponseWriter, r *http.Request)
	Set(w http.ResponseWriter, r *http.Request)
}

type StorageServiceImpl struct {
	Storage Storage
}

func (storageService StorageServiceImpl) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, getFunctionErr := storageService.Storage.Get(key)

	respMessage := "OK"
	respError := ""

	if getFunctionErr != nil {
		respMessage = "FAILED"
		respError = fmt.Sprintf("Get function error. Err: %s", getFunctionErr)
		log.Fatalf("Get function error. Err: %s", getFunctionErr)
	}

	resp := make(map[string]string)
	resp["value"] = value
	resp["message"] = respMessage
	resp["error"] = respError
	jsonResp, parseJsonErr := json.Marshal(resp)
	if parseJsonErr != nil {
		log.Fatalf("Error happened in JSON marshal. Err: %s", parseJsonErr)
	}

	if _, writeResponseErr := w.Write(jsonResp); writeResponseErr != nil {
		log.Fatalf("Write response error. Err: %s", writeResponseErr)
	}

	return
}

func (storageService StorageServiceImpl) Set(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	respMessage := "OK"
	respError := ""

	setFunctionErr := storageService.Storage.Set(key, value)
	if setFunctionErr != nil {
		respMessage = "FAILED"
		respError = fmt.Sprintf("Set function error. Err: %s", setFunctionErr)
		log.Fatalf("Set function error. Err: %s", setFunctionErr)
	}

	resp := make(map[string]string)
	resp["status"] = respMessage
	resp["error"] = respError
	jsonResp, parseJsonErr := json.Marshal(resp)
	if parseJsonErr != nil {
		log.Fatalf("Error happened in JSON marshal. Err: %s", parseJsonErr)
	}

	if _, writeResponseErr := w.Write(jsonResp); writeResponseErr != nil {
		log.Fatalf("Write response error. Err: %s", writeResponseErr)
	}

	return
}

func main() {
	var storageService StorageService
	storageService = StorageServiceImpl{Storage: StorageImpl{}}

	setUrl := fmt.Sprintf("/keys/set")
	getUrl := fmt.Sprintf("/keys/get")

	http.HandleFunc(getUrl, storageService.Get)
	http.HandleFunc(setUrl, storageService.Set)
	addr := fmt.Sprintf(":%s", serverPort)
	setListenPortError := http.ListenAndServe(addr, nil)
	log.Fatalf("Listen and serve port failed. Err: %s", setListenPortError)
}
