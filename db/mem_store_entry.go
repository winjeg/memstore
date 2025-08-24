package db

import (
	"encoding/binary"
	"strconv"
)

const entryLen = 32

// StoreEntry 一共26位存 文件存储的map里面所存的信息
type StoreEntry struct {
	FileNumber uint64
	MetaStart  uint64
	MetaEnd    uint64
	Version    uint64
}

func (se *StoreEntry) ParseStoreEntry(s []byte) *StoreEntry {
	if len(s) != entryLen {
		return nil
	}
	se.FileNumber = binary.BigEndian.Uint64(s[:8])
	se.MetaStart = binary.BigEndian.Uint64(s[8:16])
	se.MetaEnd = binary.BigEndian.Uint64(s[16:24])
	se.Version = binary.BigEndian.Uint64(s[24:32])
	return se
}

func (se *StoreEntry) ToBytes() []byte {
	result := make([]byte, 0, entryLen)
	result = binary.BigEndian.AppendUint64(result, se.FileNumber)
	result = binary.BigEndian.AppendUint64(result, se.MetaStart)
	result = binary.BigEndian.AppendUint64(result, se.MetaEnd)
	result = binary.BigEndian.AppendUint64(result, se.Version)
	return result
}

func NewStoreEntry(file string, start, end, version uint64) *StoreEntry {
	fileNumber, _ := strconv.ParseInt(file, 10, 64)
	return &StoreEntry{
		FileNumber: uint64(fileNumber),
		MetaStart:  start,
		MetaEnd:    end,
		Version:    version,
	}
}
