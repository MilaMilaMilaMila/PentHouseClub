package main

import "fmt"

type IClient interface {
	get(key string) string
	set(key string, value string) string
}

type Client struct {
}

func main() {
	fmt.Println("Hello, world!")
}
