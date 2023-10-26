package main

import (
	"PentHouseClub/internal/storage-service"
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	var app storage_service.App
	var storageService = app.Init()

	setUrl := fmt.Sprintf("/keys/set")
	getUrl := fmt.Sprintf("/keys/get")

	http.HandleFunc(getUrl, storageService.Get)
	http.HandleFunc(setUrl, storageService.Set)
	file, err := os.OpenFile("internal/storage-service/config/config.txt", os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("Open file error. Err: %s", err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("Close file error. Err: %s", err)
		}
	}()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	line := scanner.Text()
	lineElements := strings.Split(line, "=")
	addr := lineElements[1]
	setListenPortError := http.ListenAndServe(addr, nil)
	log.Printf("Listen and serve port failed. Err: %s", setListenPortError)
}
