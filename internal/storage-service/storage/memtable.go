package storage

import (
	"errors"
	"gopkg.in/OlexiyKhokhlov/avltree.v2"
	"unsafe"
)

type MemTable struct {
	AvlTree     *avltree.AVLTree[string, string]
	MaxSize     uintptr
	CurrentSize *uintptr
}

func (memTable *MemTable) Add(key string, value string) error {
	var pair = memTable.AvlTree.Find(key)
	if pair != nil {
		if *pair == value {
			return nil
		}
		memTable.AvlTree.Erase(key)
		memTable.AvlTree.Insert(key, value)
	} else {
		addSize := unsafe.Sizeof(key) + unsafe.Sizeof(value) + 8
		memTable.AvlTree.Insert(key, value)
		var newSize = *memTable.CurrentSize + addSize
		if newSize+addSize > memTable.MaxSize {
			return errors.New("MemTable size was exceeded")
		}
		*memTable.CurrentSize = newSize
	}
	return nil
}

func (memTable *MemTable) Find(key string) (string, error) {
	val := memTable.AvlTree.Find(key)
	if val == nil {
		return "", errors.New("key was not found")
	}
	return *val, nil
}

func (memTable *MemTable) Clear() {
	memTable.AvlTree.Clear()
	*memTable.CurrentSize = *new(uintptr)
}
