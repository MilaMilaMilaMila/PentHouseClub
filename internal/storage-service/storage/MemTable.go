package storage

import (
	"errors"
	"github.com/ancientlore/go-avltree"
	"unsafe"
)

type MemTable struct {
	AvlTree     *avltree.PairTree
	MaxSize     uintptr
	CurrentSize uintptr
}

func (memTable MemTable) CalculateSize() uintptr {
	return unsafe.Sizeof(memTable)
}

func (memTable MemTable) Add(key string, value string) {
	if memTable.CurrentSize > memTable.MaxSize {
		memTable.AvlTree.Clear()
	} else {
		memTable.CurrentSize += unsafe.Sizeof(key) + unsafe.Sizeof(value) + 8
		keyValue := &avltree.Pair{
			Key:   key,
			Value: value,
		}
		memTable.AvlTree.Add(*keyValue)
	}
}

func (memTable MemTable) Find(key string) (string, error) {
	pair := memTable.AvlTree.Find(key)
	if pair == nil {
		return "", errors.New("key not found")
	}
	return pair.Value.(string), nil
}
