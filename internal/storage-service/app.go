package storage_service

import (
	"PentHouseClub/internal/storage-service/service"
	"PentHouseClub/internal/storage-service/storage"
	"github.com/ancientlore/go-avltree"
)

type App struct {
	storage.Storage
	service.StorageService
}

func (app App) Init() service.StorageService {
	var storageService service.StorageService
	storageService = service.StorageServiceImpl{Storage: storage.StorageImpl{
		AvlTree: avltree.NewPairTree(avltree.AllowDuplicates),
	}}
	return storageService
}
