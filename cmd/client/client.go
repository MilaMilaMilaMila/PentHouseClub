package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

type Client interface {
	get(key string) (string, error)
	set(key string, value string) error
}

type ClientImpl struct {
	baseUrl string
}

func NewClientImpl(baseUrl string) *ClientImpl {
	return &ClientImpl{baseUrl: baseUrl}
}

type RespJson struct {
	Value   string `json:"value"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

func main() {
	file, err := os.OpenFile("config.txt", os.O_RDONLY, 0644)
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
	URL := lineElements[1]
	var client Client
	client = ClientImpl{URL}
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
		resp, getResponseError := client.get(args[1])
		if getResponseError != nil {
			fmt.Println(getResponseError.Error())
		}
		fmt.Println(resp)
	} else if args[0] == "set" {
		if len(args) < 3 {
			fmt.Println("Invalid arguments. Key and RespJson are required")
			os.Exit(1)
		}
		setResponseError := client.set(args[1], args[2])
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

func (client ClientImpl) get(key string) (string, error) {
	url := fmt.Sprintf("%s/keys/get", client.baseUrl)
	req, createRequestError := http.NewRequest(http.MethodGet, url, nil)
	if createRequestError != nil {
		log.Print(createRequestError)
		os.Exit(1)
	}
	q := req.URL.Query()
	q.Add("key", key)
	req.URL.RawQuery = q.Encode()

	clientR := &http.Client{}
	resp, makeRequestError := clientR.Do(req)
	if makeRequestError != nil {
		panic(makeRequestError)
	}

	var respJson RespJson
	if getResponseErr := json.NewDecoder(resp.Body).Decode(&respJson); getResponseErr != nil {
		log.Fatalf("Get response json error. Err: %s", getResponseErr)
	}

	defer func() {
		closeResponseError := resp.Body.Close()
		if closeResponseError != nil {
			log.Fatalf("Close response body error. Err: %s", closeResponseError)
		}
	}()
	if respJson.Message != "OK" {
		return respJson.Message, errors.New(respJson.Error)
	}
	return respJson.Value, nil
}

func (client ClientImpl) set(key, value string) error {
	url := fmt.Sprintf("%s/keys/set", client.baseUrl)
	req, createRequestError := http.NewRequest(http.MethodPut, url, nil)
	if createRequestError != nil {
		log.Print(createRequestError)
		os.Exit(1)
	}

	q := req.URL.Query()
	q.Add("key", key)
	q.Add("value", value)
	req.URL.RawQuery = q.Encode()

	clientR := &http.Client{}
	resp, doRequestErr := clientR.Do(req)
	if doRequestErr != nil {
		return doRequestErr
	}

	defer func() {
		closeResponseError := resp.Body.Close()
		if closeResponseError != nil {
			log.Fatalf("Close response body error. Err: %s", closeResponseError)
		}
	}()

	if resp.Status != "200 OK" {
		fmt.Println(resp.Status)
	}
	return nil
}
