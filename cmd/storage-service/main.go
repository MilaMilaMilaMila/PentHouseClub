package main

import (
	"PentHouseClub/internal/storage-service"
	"PentHouseClub/internal/storage-service/config"
	"fmt"
	"os"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var app storage_service.App
	if cfg.Type == "avlTree" {
		app.Start(*cfg)
		fmt.Println("Starting avl tree")
	} else {
		app.StartRedis(*cfg)
	}

}
