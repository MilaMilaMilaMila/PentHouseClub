package storage

type Storage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}

type StorageImpl struct {
	MemTable MemTable
}

func (storage StorageImpl) Set(key string, value string) error {
	//file, err := os.OpenFile("internal/storage-service/storage/data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	//if err != nil {
	//	return err
	//}
	//defer func() {
	//	if err = file.Close(); err != nil {
	//		log.Printf("Close file error. Err: %s", err)
	//	}
	//}()
	//
	//line := "key=" + key + " " + "value=" + value + "\n"
	storage.MemTable.Add(key, value)
	//_, writeError := file.WriteString(line)
	//if writeError != nil {
	//	log.Printf("Write data in file error. Err: %s", writeError)
	//	return writeError
	//}
	return nil
}

func (storage StorageImpl) Get(key string) (string, error) {
	//file, err := os.OpenFile("internal/storage-service/storage/data.txt", os.O_RDONLY, 0644)
	//if err != nil {
	//	return "", err
	//}
	//defer func() {
	//	if err = file.Close(); err != nil {
	//		log.Printf("Close file error. Err: %s", err)
	//	}
	//}()
	//
	//flag := false
	//value := ""
	//var keyError error
	//
	//scanner := bufio.NewScanner(file)
	//for scanner.Scan() {
	//	line := scanner.Text()
	//	lineElements := strings.Split(line, " ")
	//	if len(line) == 0 {
	//		keyError = errors.New(fmt.Sprintf("Key %s does not exist", key))
	//		log.Printf("Not existing key error. Err: %s", keyError)
	//		return value, keyError
	//	}
	//	storageKey := strings.Split(lineElements[0], "=")[1]
	//	storageValue := strings.Split(lineElements[1], "=")[1]
	//
	//	if storageKey == key {
	//		value = storageValue
	//		flag = true
	//	}
	//}
	//
	//if !flag {
	//	keyError = errors.New(fmt.Sprintf("Key %s does not exist", key))
	//	log.Printf("Not existing key error. Err: %s", keyError)
	//} else {
	//	keyError = nil
	//}
	//
	//if scannerErr := scanner.Err(); err != nil {
	//	log.Printf("Scanner error. Err: %s", scannerErr)
	//	return "", scannerErr
	//}
	var val, err = storage.MemTable.Find(key)
	return val, err
}
