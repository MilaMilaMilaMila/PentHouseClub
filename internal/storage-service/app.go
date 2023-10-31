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

func (app App) Init(configInfo config.LSMconfig, memTable storage.MemTable, journalPath string, ssTables *[]storage.SsTable) service.StorageService {
	var storageService service.StorageService
	dirPath := filepath.Join(GetWorkDirAbsPath(), configInfo.SSTDir)
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating ssTables dir. Err: %s", err)
	}
	err = os.MkdirAll(journalPath, 0777)
	if err != nil {
		log.Printf("error occuring while creating journal dir. Err: %s", err)
	}
	merger := storage.MergerImpl{
		MemNewFileLimit:      memTable.MaxSize,
		StorageSstDirPath:    dirPath,
		SsTableSegmentLength: configInfo.SSTsegLen,
	}
	storage := storage.StorageImpl{
		MemTable:             memTable,
		SsTableSegmentLength: configInfo.SSTsegLen,
		SsTableDir:           dirPath,
		SsTables:             ssTables,
		JournalPath:          journalPath,
		Merger:               merger,
		MergePeriodSec:       configInfo.GCperiodSec,
	}
	go storage.GC()
	storageService = service.StorageServiceImpl{Storage: &storage}

	return storageService
}

func (app App) Start(configInfo config.LSMconfig) service.StorageService {
	var memTable = storage.MemTable{AvlTree: avltree.NewAVLTreeOrderedKey[string, string](),
		MaxSize:  configInfo.MtSize,
		CurrSize: new(uintptr)}
	journalPath := filepath.Join(GetWorkDirAbsPath(), configInfo.JPath)
	journalName, _ := os.ReadDir(journalPath)
	if len(journalName) != 0 {
		log.Printf("Restoring AVL tree")
		memTable.AvlTree, *memTable.CurrSize = app.RestoreAvlTree(filepath.Join(journalPath, journalName[0].Name()))
	}
	os.RemoveAll(journalPath)
	ssTablesDir := filepath.Join(GetWorkDirAbsPath(), configInfo.SSTDir)
	ssTablesJournalPath := filepath.Join(ssTablesDir, "journal")
	ssTablesjournalName, _ := os.ReadDir(ssTablesJournalPath)
	var ssTables = new([]storage.SsTable)
	if len(ssTablesjournalName) != 0 {
		log.Printf("Restoring ssTables")
		for _, journal := range ssTablesjournalName {
			journalPath := filepath.Join(ssTablesJournalPath, journal.Name())
			ssTableName := filepath.Join(ssTablesDir, journal.Name())
			*ssTables = append(*ssTables, storage.Restore(ssTableName, journalPath, journal.Name()))
		}
	}
	return app.Init(configInfo, memTable, journalPath, ssTables)
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
