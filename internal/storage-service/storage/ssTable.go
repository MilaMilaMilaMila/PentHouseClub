package storage

type ssTable struct {
	dirPath       string
	segmentLength int
	sparseIndex   map[int64]string
}

func (table ssTable) Add(memTable MemTable) {

}

func (table ssTable) Find(key string) string {

}

func (table ssTable) buildIndex() map[int64]string {

}
