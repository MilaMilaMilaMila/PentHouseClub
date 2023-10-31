package main

import (
	"PentHouseClub/internal/storage-service"
	"PentHouseClub/internal/storage-service/config"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"net/http"
)

func readConfig() *config.LSMconfig {
	conf := config.New()
	return conf
}

func main() {
	conf := readConfig()
	var app storage_service.App
	var storageService = app.Start(*conf)
	viper.SetDefault("listen", ":8080")
	setUrl := fmt.Sprintf("/keys/set")
	getUrl := fmt.Sprintf("/keys/get")

	http.HandleFunc(getUrl, storageService.Get)
	http.HandleFunc(setUrl, storageService.Set)

	//line := scanner.Text()
	//lineElements := strings.Split(line, "=")
	//addr := lineElements[1]
	setListenPortError := http.ListenAndServe(viper.GetString("listen"), nil)
	log.Printf("Listen and serve port failed. Err: %s", setListenPortError)
}
