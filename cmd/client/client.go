package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

const baseURL = "http://localhost:8080"

type Client interface {
	get(key string) (string, error)
	set(key string, value string) error
}

type ClientImpl struct {
}

func main() {
	var client Client
	client = ClientImpl{}
	if len(os.Args) == 1 {
		os.Exit(3)
	}
	args := os.Args[1:]
	if args[0] == "get" {
		if len(args) < 2 {
			fmt.Println("Invalid arguments. Key is required")
			os.Exit(1)
		}
		resp, err := client.get(args[1])
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println(resp)
	} else if args[0] == "set" {
		if len(args) < 3 {
			fmt.Println("Invalid arguments. Key and RespJson are required")
			os.Exit(1)
		}
		err := client.set(args[1], args[2])
		fmt.Println("here")
		if err != nil {
			fmt.Println(err.Error())
		}
	} else {
		fmt.Println("Invalid arguments")
		os.Exit(1)
	}
}

func (client ClientImpl) get(key string) (string, error) {
	url := fmt.Sprintf("%s/keys/get", baseURL)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	q := req.URL.Query()
	q.Add("key", key)
	req.URL.RawQuery = q.Encode()
	fmt.Println(req.URL.String())

	clientR := &http.Client{}
	resp, err1 := clientR.Do(req)
	if err1 != nil {
		panic(err1)
	}

	type RespJson struct {
		Value   string `json:"value"`
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	var respJson RespJson
	if getResponseErr := json.NewDecoder(resp.Body).Decode(&respJson); getResponseErr != nil {
		log.Fatalf("Get response json error. Err: %s", getResponseErr)
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			log.Fatalf("Close response body error. Err: %s", err)
		}
	}()

	return respJson.Value, nil
}

func (client ClientImpl) set(key, value string) error {
	url := fmt.Sprintf("%s/keys/set", baseURL)
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	q := req.URL.Query()
	q.Add("key", key)
	q.Add("value", value)
	req.URL.RawQuery = q.Encode()
	fmt.Println(req.URL.String())

	clientR := &http.Client{}
	resp, doRequestErr := clientR.Do(req)
	if doRequestErr != nil {
		return doRequestErr
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			log.Fatalf("Close response body error. Err: %s", err)
		}
	}()

	return nil
}
