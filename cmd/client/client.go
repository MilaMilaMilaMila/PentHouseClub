package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

const baseURL = "http://localhost:8080"

type IClient interface {
	get(key string) (string, error)
	set(key string, value string) error
}

type Client struct {
}

func main() {
	var client Client
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
			fmt.Println("Invalid arguments. Key and value are required")
			os.Exit(1)
		}
		err := client.set(args[1], args[2])
		if err != nil {
			fmt.Println(err.Error())
		}
	} else {
		fmt.Println("Invalid arguments")
		os.Exit(1)
	}
}

func (client Client) get(key string) (string, error) {
	url := fmt.Sprintf("%s/keys/$%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (client Client) set(key, value string) error {
	url := fmt.Sprintf("%s/keys/$%s?value=$%s", baseURL, key, value)
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		return err
	}

	clientR := &http.Client{}
	_, err = clientR.Do(req)
	if err != nil {
		return err
	}

	return nil
}
