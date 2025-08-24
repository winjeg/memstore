package db

import (
	"gitlab.bitmartpro.com/spot-go/file-store/store"

	"errors"
	"fmt"
	"math"
	"os"
)

var ErrKeyNotFound = errors.New("error key not found")
var ErrDBClosed = errors.New("error database is closed")

type KVUnit struct {
	FilePrefix string
	data       *store.DataFile
	meta       *store.MetaFile
	logger     store.Logger
}

func (kv *KVUnit) Close() error {
	if err := kv.meta.Close(); err != nil {
		return err
	}
	return kv.data.Close()
}

func (kv *KVUnit) Sync() error {
	if err := kv.data.Sync(); err != nil {
		return err
	}
	return kv.meta.Sync()
}

func (kv *KVUnit) Set(k, v []byte, version uint64) (*store.Pos, error) {
	if kv == nil || kv.data == nil {
		fmt.Printf("error setting, kv: %+v", kv)
	}
	if pos, err := kv.data.WriteData(&store.DataEntry{Key: k, Value: v}); err != nil {
		return nil, err
	} else {
		if pos, err := kv.meta.WriteMeta(&store.EntryMeta{
			KeyStart:   uint64(pos.KeyStart),
			KeyEnd:     uint64(pos.KeyEnd),
			ValueStart: uint64(pos.ValueStart),
			ValueEnd:   uint64(pos.ValueEnd),
			Meta:       0,
			ExpireTime: math.MaxInt64,
			Version:    version,
		}); err != nil {
			return nil, err
		} else {
			return pos, nil
		}
	}
}

func (kv *KVUnit) GetMeta(start, end uint64) ([]byte, error) {
	return kv.meta.Read(start, end)
}

func (kv *KVUnit) Get(start, end uint64) ([]byte, error) {
	entryData, metaErr := kv.GetMeta(start, end)
	if metaErr != nil {
		return nil, metaErr
	}
	meta := &store.EntryMeta{}
	meta = meta.FromBytes(entryData)
	if meta == nil || meta.Meta == 1 {
		return nil, ErrKeyNotFound
	}
	entry, dataErr := kv.data.Read(&store.DataPos{
		KeyStart:   int64(meta.KeyStart),
		KeyEnd:     int64(meta.KeyEnd),
		ValueStart: int64(meta.ValueStart),
		ValueEnd:   int64(meta.ValueEnd),
	})
	if dataErr != nil {
		return nil, dataErr
	}
	return entry.Value, nil
}

func (kv *KVUnit) Delete(k []byte, version uint64) error {
	if pos, err := kv.data.WriteData(&store.DataEntry{Key: k, Value: []byte("DL")}); err != nil {
		return err
	} else {
		if _, err := kv.meta.WriteMeta(&store.EntryMeta{
			KeyStart:   uint64(pos.KeyStart),
			KeyEnd:     uint64(pos.KeyEnd),
			ValueStart: uint64(pos.ValueStart),
			ValueEnd:   uint64(pos.ValueEnd),
			Meta:       1,
			ExpireTime: 0,
			Version:    version,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (kv *KVUnit) Range(f func(k, v []byte, meta *store.EntryMeta, start, end uint64)) {
	kv.meta.LoadAll(func(bytes []byte, start, end uint64) {
		meta := &store.EntryMeta{}
		meta.FromBytes(bytes)
		entry, dataErr := kv.data.Read(&store.DataPos{
			KeyStart:   int64(meta.KeyStart),
			KeyEnd:     int64(meta.KeyEnd),
			ValueStart: int64(meta.ValueStart),
			ValueEnd:   int64(meta.ValueEnd),
		})
		if entry == nil || dataErr != nil { // 没读到
			if kv.logger != nil {
				kv.logger.Warnf("no data is read, k_start :%d, k_end: %d, v_start: %d, v_end: %d",
					meta.KeyStart, meta.KeyEnd, meta.ValueStart, meta.ValueEnd)
			}
		} else {
			f(entry.Key, entry.Value, meta, start, end)
		}
	})
}

func (kv *KVUnit) RangeKey(f func(k []byte, meta *store.EntryMeta, start, end uint64)) {
	kv.meta.LoadAll(func(bytes []byte, start, end uint64) {
		meta := &store.EntryMeta{}
		meta.FromBytes(bytes)
		key := kv.data.ReadKey(meta.KeyStart, meta.KeyEnd)
		f(key, meta, start, end)
	})
}

// Remove 关闭 此kv单元，然后删除此kv单元
func (kv *KVUnit) Remove() error {
	if err := kv.Close(); err != nil {
		return err
	}
	if err := os.Remove(fmt.Sprintf("%s.data", kv.FilePrefix)); err != nil {
		return err
	}
	if err := os.Remove(fmt.Sprintf("%s.meta", kv.FilePrefix)); err != nil {
		return err
	}
	return nil
}

func NewKVUnit(prefix string, logger store.Logger) *KVUnit {
	dataFile := store.NewDataFile(fmt.Sprintf("%s.data", prefix), logger)
	metaData := store.NewMetaFile(fmt.Sprintf("%s.meta", prefix), logger)
	return &KVUnit{
		FilePrefix: prefix,
		data:       dataFile,
		meta:       metaData,
	}
}
