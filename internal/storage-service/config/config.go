package config

type DataSizeRestriction struct {
	MemTableMaxSize         uintptr
	SsTableSegmentMaxLength int64
	SsTableDir              string
}
