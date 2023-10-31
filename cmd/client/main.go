package main

import (
	client2 "PentHouseClub/internal/client"
	"PentHouseClub/internal/client/config"
	"fmt"
	"github.com/caarlos0/env/v9"
	"os"
)

func main() {
	var cfg config.Address

	err := env.Parse(&cfg)
	if err != nil {
		fmt.Println(err.Error())
	}
	var client client2.Client
	client = client2.ClientImpl{BaseUrl: "http://" + cfg.Host + ":" + cfg.Port}
	if len(os.Args) <= 1 {
		fmt.Println("Invalid arguments. Key is required")
		os.Exit(1)
	}
	args := os.Args[1:]
	if args[0] == "get" {
		if len(args) < 2 {
			fmt.Println("Invalid arguments. Key is required")
			os.Exit(1)
		}
		resp, getResponseError := client.Get(args[1])
		if getResponseError != nil {
			fmt.Println(getResponseError.Error())
		}
		fmt.Println(resp)
	} else if args[0] == "set" {
		if len(args) < 3 {
			fmt.Println("Invalid arguments. Key and RespJson are required")
			os.Exit(1)
		}
		setResponseError := client.Set(args[1], args[2])
		if setResponseError != nil {
			fmt.Println(setResponseError)
			fmt.Println(setResponseError.Error())
		} else {
			fmt.Println("Entry was added successfully")
		}
	} else {
		fmt.Println("Invalid arguments")
		os.Exit(1)
	}
}
