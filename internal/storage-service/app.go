package storage_service

import (
	"bufio"
	"strconv"

	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"PentHouseClub/internal/storage-service/config"
	"PentHouseClub/internal/storage-service/service"
	"PentHouseClub/internal/storage-service/storage"
	"PentHouseClub/internal/storage-service/storage/impl"
	"github.com/spf13/viper"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
)

type StorageService interface {
	Get(w http.ResponseWriter, r *http.Request)
	Set(w http.ResponseWriter, r *http.Request)
}

type Storage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	GC()
}

type App struct {
	StorageService
}

func (app App) Init(configInfo config.LSMconfig, memTables []storage.MemTable, journalPath string, ssTables map[int][]storage.SsTable) {
	dirPath := filepath.Join(GetWorkDirAbsPath(), configInfo.SSTDir)
	storageService := service.NewSharder(configInfo, memTables, journalPath, ssTables, dirPath)
	viper.SetDefault("listen", ":8080")
	setUrl := fmt.Sprintf("/keys/set")
	getUrl := fmt.Sprintf("/keys/get")

	http.HandleFunc(getUrl, storageService.Get)
	http.HandleFunc(setUrl, storageService.Set)
}

func (app App) Start(configInfo config.LSMconfig) {
	var memTables []storage.MemTable
	journalPath := filepath.Join(GetWorkDirAbsPath(), configInfo.JPath)
	var ssTables map[int][]storage.SsTable
	for i := 0; i < configInfo.SectionCount; i++ {
		var memTable = storage.MemTable{AvlTree: avltree.NewAVLTreeOrderedKey[string, string](),
			MaxSize:  configInfo.MtSize,
			CurrSize: new(uintptr)}
		newJournalPath := journalPath + strconv.Itoa(i)
		journalName, _ := os.ReadDir(newJournalPath)
		if len(journalName) != 0 {
			log.Printf("Restoring AVL tree")
			memTable.AvlTree, *memTable.CurrSize = app.RestoreAvlTree(filepath.Join(newJournalPath, journalName[0].Name()))
		}
		ssTablesDir := filepath.Join(GetWorkDirAbsPath(), configInfo.SSTDir) + strconv.Itoa(i)
		ssTablesJournalPath := filepath.Join(ssTablesDir, "journal")
		ssTablesjournalName, _ := os.ReadDir(ssTablesJournalPath)
		if len(ssTablesjournalName) != 0 {
			log.Printf("Restoring ssTables")
			for _, journal := range ssTablesjournalName {
				journalPath := filepath.Join(ssTablesJournalPath, journal.Name())
				ssTableName := filepath.Join(ssTablesDir, journal.Name())
				ssTables[i] = append(ssTables[i], storage.Restore(ssTableName, journalPath, journal.Name()))
			}
		}
		memTables = append(memTables, memTable)
	}
	app.Init(configInfo, memTables, journalPath, ssTables)

	setListenPortError := http.ListenAndServe(viper.GetString("listen"), nil)
	log.Printf("Listen and serve port failed. Err: %s", setListenPortError)
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

func (a App) StartRedis(cfg config.LSMconfig) {

	r := impl.NewRedis(cfg.RedisTime)

	r.Connect()
	storageService := service.StorageServiceImpl{Storage: r}
	viper.SetDefault("listen", ":8080")
	setUrl := fmt.Sprintf("/keys/set")
	getUrl := fmt.Sprintf("/keys/get")

	http.HandleFunc(getUrl, storageService.Get)
	http.HandleFunc(setUrl, storageService.Set)

	setListenPortError := http.ListenAndServe(viper.GetString("listen"), nil)
	log.Printf("Listen and serve port failed. Err: %s", setListenPortError)

}
