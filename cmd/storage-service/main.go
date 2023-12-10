package main

import (
	"PentHouseClub/internal/storage-service"
	"PentHouseClub/internal/storage-service/config"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"os"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var app storage_service.App
	var storageService = app.Start(*cfg)
	viper.SetDefault("listen", ":8080")
	setUrl := fmt.Sprintf("/keys/set")
	getUrl := fmt.Sprintf("/keys/get")

	http.HandleFunc(getUrl, storageService.Get)
	http.HandleFunc(setUrl, storageService.Set)

	setListenPortError := http.ListenAndServe(viper.GetString("listen"), nil)
	log.Printf("Listen and serve port failed. Err: %s", setListenPortError)
}
