package store

import (
	"encoding/binary"
	"hash/crc32"
)

const entryLen = 56

type Pos struct {
	Start uint64
	End   uint64
}

// EntryMeta 50 Bytes, Total.
type EntryMeta struct {
	KeyStart   uint64
	KeyEnd     uint64
	ValueStart uint64
	ValueEnd   uint64
	ExpireTime uint64
	Version    uint64
	Meta       uint32
	CheckSum   uint32
}

func (m *EntryMeta) ToBytes() []byte {
	result := make([]byte, 0, entryLen)
	result = binary.BigEndian.AppendUint64(result, m.KeyStart)
	result = binary.BigEndian.AppendUint64(result, m.KeyEnd)
	result = binary.BigEndian.AppendUint64(result, m.ValueStart)
	result = binary.BigEndian.AppendUint64(result, m.ValueEnd)
	result = binary.BigEndian.AppendUint64(result, m.ExpireTime) // 结构体第5字段
	result = binary.BigEndian.AppendUint64(result, m.Version)    // 结构体第6字段
	result = binary.BigEndian.AppendUint32(result, m.Meta)       // 结构体第7字段
	checksum := crc32.ChecksumIEEE(result)
	result = binary.BigEndian.AppendUint32(result, checksum)
	return result
}

func (m *EntryMeta) FromBytes(entryData []byte) *EntryMeta {
	if len(entryData) != entryLen {
		return nil
	}
	expected := binary.BigEndian.Uint32(entryData[entryLen-4:])
	if crc32.ChecksumIEEE(entryData[:entryLen-4]) != expected {
		return nil // 数据损坏
	}
	m.KeyStart = binary.BigEndian.Uint64(entryData[:8])      // 0-7
	m.KeyEnd = binary.BigEndian.Uint64(entryData[8:16])      // 8-15
	m.ValueStart = binary.BigEndian.Uint64(entryData[16:24]) // 16-23
	m.ValueEnd = binary.BigEndian.Uint64(entryData[24:32])   // 24-31
	m.ExpireTime = binary.BigEndian.Uint64(entryData[32:40]) // 32-39 (结构体第5字段)
	m.Version = binary.BigEndian.Uint64(entryData[40:48])    // 40-47 (结构体第6字段)
	m.Meta = binary.BigEndian.Uint32(entryData[48:52])       // 48-49 (结构体第7字段)   // 结构体第7字段
	return m
}

// IsDeleted 使用第一位来表示是否被删除
func (m *EntryMeta) IsDeleted() bool {
	return m.Meta&1 == 1
}
