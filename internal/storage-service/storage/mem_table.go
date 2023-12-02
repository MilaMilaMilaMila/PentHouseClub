package storage

import (
	"errors"
	"unsafe"

	"gopkg.in/OlexiyKhokhlov/avltree.v2"
)

type MemTable struct {
	AvlTree  *avltree.AVLTree[string, string]
	MaxSize  uintptr
	CurrSize *uintptr
}

func (m *MemTable) Add(key string, value string) error {
	var pair = m.AvlTree.Find(key)

	if pair != nil {
		if *pair == value {
			return nil
		}
		m.AvlTree.Erase(key)
		m.AvlTree.Insert(key, value)
	} else {
		addSize := unsafe.Sizeof(key) + unsafe.Sizeof(value) + 8
		m.AvlTree.Insert(key, value)
		var newSize = *m.CurrSize + addSize
		if newSize+addSize > m.MaxSize {
			return errors.New("MemTable size was exceeded")
		}
		*m.CurrSize = newSize
	}

	return nil
}

func (m *MemTable) Find(key string) (string, error) {
	val := m.AvlTree.Find(key)
	if val == nil {
		return "", errors.New("key was not found")
	}
	return *val, nil
}

func (m *MemTable) Clear() {
	m.AvlTree.Clear()
	*m.CurrSize = *new(uintptr)
}
