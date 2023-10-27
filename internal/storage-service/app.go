package storage_service

import (
	"PentHouseClub/internal/storage-service/config"
	"PentHouseClub/internal/storage-service/service"
	"PentHouseClub/internal/storage-service/storage"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
	"os"
	"path/filepath"
)

type App struct {
	storage.Storage
	service.StorageService
}

func (app App) Init(configInfo config.DataSizeRestriction) service.StorageService {
	var storageService service.StorageService
	currentDir, _ := os.Getwd()
	f, _ := filepath.Abs(currentDir)
	dirPath := filepath.Join(f, configInfo.SsTableDir)
	os.Mkdir(dirPath, 0777)
	storageService = service.StorageServiceImpl{Storage: storage.StorageImpl{
		MemTable: storage.MemTable{AvlTree: avltree.NewAVLTreeOrderedKey[string, string](),
			MaxSize:     configInfo.MemTableMaxSize,
			CurrentSize: new(uintptr)},
		SsTableSegmentLength: configInfo.SsTableSegmentMaxLength,
		SsTableDir:           dirPath,
		SsTables:             new([]storage.SsTable),
	}}

	return storageService
}
