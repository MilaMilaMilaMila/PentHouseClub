package storage_service

import (
	"PentHouseClub/internal/storage-service/config"
	"PentHouseClub/internal/storage-service/service"
	"PentHouseClub/internal/storage-service/storage"
	"bufio"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

type App struct {
	storage.Storage
	service.StorageService
}

func (app App) Init(configInfo config.DataSizeRestriction, memTable storage.MemTable, journalPath string) service.StorageService {
	var storageService service.StorageService
	dirPath := filepath.Join(GetWorkDirAbsPath(), configInfo.SsTableDir)
	err := os.Mkdir(dirPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating ssTables dir. Err: %s", err)
	}
	err = os.Mkdir(journalPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating journal dir. Err: %s", err)
	}
	storageService = service.StorageServiceImpl{Storage: storage.StorageImpl{
		MemTable:             memTable,
		SsTableSegmentLength: configInfo.SsTableSegmentMaxLength,
		SsTableDir:           dirPath,
		SsTables:             new([]storage.SsTable),
		JournalPath:          journalPath,
	}}

	return storageService
}

func (app App) Start(configInfo config.DataSizeRestriction) service.StorageService {
	var memTable = storage.MemTable{AvlTree: avltree.NewAVLTreeOrderedKey[string, string](),
		MaxSize:     configInfo.MemTableMaxSize,
		CurrentSize: new(uintptr)}
	journalPath := filepath.Join(GetWorkDirAbsPath(), configInfo.JournalPath)
	journalName := storage.GetFileNameInDir(journalPath)
	if journalName != "" {
		log.Printf("Restoring AVL tree")
		memTable.AvlTree, *memTable.CurrentSize = app.RestoreAvlTree(filepath.Join(journalPath, journalName))
	}
	os.RemoveAll(journalPath)
	return app.Init(configInfo, memTable, journalPath)
}

func (app App) RestoreAvlTree(journalPath string) (*avltree.AVLTree[string, string], uintptr) {
	f, err := os.Open(journalPath)
	if err != nil {
		log.Printf("Open journal error. Err: %s", err)
		return avltree.NewAVLTreeOrderedKey[string, string](), uintptr(0)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Printf("Close journal error. Err: %s", err)
		}
	}()
	var avlTree = avltree.NewAVLTreeOrderedKey[string, string]()
	sc := bufio.NewScanner(f)
	var size = *new(uintptr)
	for sc.Scan() {
		var logInfo = strings.Split(strings.Split(sc.Text(), ".")[0], ":")
		var key = logInfo[1][1:]
		var value = logInfo[2]
		var pair = avlTree.Find(key)
		if pair != nil {
			if *pair != value {
				avlTree.Erase(key)
				avlTree.Insert(key, value)
			}
		} else {
			size += unsafe.Sizeof(key) + unsafe.Sizeof(value) + 8
			avlTree.Insert(key, value)
		}
	}
	return avlTree, size
}

func GetWorkDirAbsPath() string {
	currentDir, _ := os.Getwd()
	f, _ := filepath.Abs(currentDir)
	return f
}
