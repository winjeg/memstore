package store

import "hash/crc32"

type DataPos struct {
	KeyStart   int64
	KeyEnd     int64
	ValueStart int64
	ValueEnd   int64
}

type DataEntry struct {
	Key   []byte
	Value []byte
}

func (e *DataEntry) Len() int {
	return len(e.Key) + len(e.Value) + 4
}

func (e *DataEntry) CalPosition(start int64) *DataPos {
	if e.Key == nil {
		return nil
	}
	keyEnd := start + int64(len(e.Key))
	return &DataPos{
		KeyStart:   start,
		KeyEnd:     keyEnd,
		ValueStart: keyEnd,
		ValueEnd:   keyEnd + int64(len(e.Value)) + 4,
	}
}

func (e *DataEntry) CheckSum() uint32 {
	if e.Key == nil {
		return 0
	}
	hash := crc32.NewIEEE()
	_, _ = hash.Write(e.Key)
	_, _ = hash.Write(e.Value)
	return hash.Sum32()
}

func (e *DataEntry) FromBytes(keyData, valueData []byte) *DataEntry {
	return &DataEntry{
		Key:   keyData,
		Value: valueData,
	}
}
