package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

type Client interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}

type ClientImpl struct {
	BaseUrl string
}

type RespJson struct {
	Value   string `json:"value"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

func (client ClientImpl) Get(key string) (string, error) {
	url := fmt.Sprintf("%s/keys/get", client.BaseUrl)
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

func (client ClientImpl) Set(key, value string) error {
	url := fmt.Sprintf("%s/keys/set", client.BaseUrl)
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
