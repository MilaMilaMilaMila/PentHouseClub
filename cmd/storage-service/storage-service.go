package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type IStorageService interface {
	get(key string) (string, error)
	set(key string, value string) error
}

type StorageService struct {
}

func (storageService StorageService) set(key string, value string) error {
	file, err := os.OpenFile("cmd/storage-service/data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	line := "key=" + key + " " + "value=" + value
	_, writeError := file.WriteString(line)
	if writeError != nil {
		log.Fatal(writeError)
		return writeError
	}
	return nil
}

func (storageService StorageService) get(key string) (string, error) {
	file, err := os.OpenFile("cmd/storage-service/data.txt", os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	flag := false
	value := ""
	var keyError error

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lineElements := strings.Split(line, " ")
		storageKey := strings.Split(lineElements[0], "=")[1]
		storageValue := strings.Split(lineElements[1], "=")[1]

		if storageKey == key {
			value = storageValue
			flag = true
		}

		//fmt.Println(storageKey)
		//fmt.Println(storageValue)
	}

	if !flag {
		// здесь надо ошибку настроить типо нет совпадающего ключа
		// может надо кастомную ошибку, я не знаю
		keyError = os.ErrNotExist
	} else {
		keyError = nil
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return value, keyError
}

func main() {
	var storageService IStorageService
	storageService = StorageService{}
	err1 := storageService.set("milasha", "Vorobusha")
	if err1 != nil {
		log.Fatal(err1)
	}
	value, err2 := storageService.get("milasha")
	if err2 != nil {
		log.Fatal(err2)
	}
	fmt.Println(value)
}
