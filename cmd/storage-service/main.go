package main

import (
	"PentHouseClub/internal/storage-service"
	"PentHouseClub/internal/storage-service/config"
	"bufio"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"os"
)

const configPath = "internal/storage-service/config/config.yaml"

func readConfig() config.DataSizeRestriction {
	viper.SetConfigFile(configPath)
	err := viper.ReadInConfig()
	if err != nil {
		log.Panicf("Unable to read config file: %s", err)
	}
	dataSizeRestriction := config.DataSizeRestriction{MemTableMaxSize: uintptr(viper.GetInt("MemTableSize"))}
	return dataSizeRestriction
}

func main() {
	dataSizeRestriction := readConfig()
	var app storage_service.App
	var storageService = app.Init(dataSizeRestriction.MemTableMaxSize)
	viper.SetDefault("listen", ":8080")
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
	//line := scanner.Text()
	//lineElements := strings.Split(line, "=")
	//addr := lineElements[1]
	setListenPortError := http.ListenAndServe(viper.GetString("listen"), nil)
	log.Printf("Listen and serve port failed. Err: %s", setListenPortError)
}
