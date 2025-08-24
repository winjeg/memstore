package store

import (
	"gitlab.bitmartpro.com/spot-go/file-store/store/amap"

	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

const dataMaxSize = 256 << 20 // 默认512MB 30~40w条数据 3~5秒

type DataFile struct {
	file    *os.File
	data    *[dataMaxSize]byte
	dataRef []byte
	lock    sync.Mutex
	dataEnd atomic.Uint64 // data结束的位置， 开始的位置为0
	closed  atomic.Bool
	logger  Logger
}

func NewDataFile(file string, logger Logger) *DataFile {
	f, _ := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	fkv := &DataFile{file: f, lock: sync.Mutex{}, closed: atomic.Bool{}, logger: logger}
	if err := fkv.init(); err != nil {
		if fkv.logger != nil {
			fkv.logger.Errorln("error init meta file")
		}
		return nil
	}
	return fkv
}

func (f *DataFile) mmap() error {
	// 1. 按页大小对齐映射长度
	pageSize := os.Getpagesize()
	alignedSize := (dataMaxSize + pageSize - 1) &^ (pageSize - 1)
	b, err := amap.MMap(f.file, true, int64(alignedSize))
	addr := uintptr(unsafe.Pointer(&b[0]))
	if err != nil {
		return err
	}
	f.dataRef = b
	f.data = (*[dataMaxSize]byte)(unsafe.Pointer(addr))
	return nil
}

func (f *DataFile) grow(size int64) error {
	if info, _ := f.file.Stat(); info.Size() >= size {
		return nil
	}
	if err := f.file.Truncate(size); err != nil {
		return err
	}
	return nil
}

func (f *DataFile) Unmap() error {
	if err := amap.Munmap(f.dataRef); err != nil {
		return err
	}
	f.data = nil
	f.dataRef = nil
	return nil
}

func (f *DataFile) Close() error {
	if f.closed.Load() {
		return fmt.Errorf("meta file: %s closed", f.file.Name())
	}
	if f.closed.CompareAndSwap(false, true) {
		err := f.Unmap()
		f.file.Close()
		return err
	}
	return fmt.Errorf("already closing: %s", f.file.Name())
}

func (f *DataFile) Sync() error {
	if f.closed.Load() {
		return fmt.Errorf("file :%s, closed", f.file.Name())
	}
	return amap.Msync(f.dataRef)
}

func (f *DataFile) init() error {
	if err := f.mmap(); err != nil {
		return err
	}
	if err := f.grow(dataMaxSize); err != nil {
		return err
	}
	dataEnd := binary.BigEndian.Uint64(f.data[:int(8)])
	f.dataEnd.Store(uint64(int64(dataEnd)))
	return nil
}
func (f *DataFile) WriteData(entry *DataEntry) (*DataPos, error) {
	if f.closed.Load() {
		return nil, fmt.Errorf("file :%s, closed", f.file.Name())
	}
	if entry == nil || len(entry.Key) == 0 {
		return nil, errors.New("entry can't be nil")
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	dataEnd := binary.BigEndian.Uint64(f.data[:int(8)])
	if dataEnd == 0 {
		dataEnd = 8
	}
	totalLen := int64(dataEnd) + int64(entry.Len())
	if totalLen >= int64(dataMaxSize) {
		return nil, ErrDatabaseFull
	}
	pos := entry.CalPosition(int64(dataEnd))
	// 写入数据
	for i, b := range entry.Key {
		f.data[int(pos.KeyStart)+i] = b
	}

	for i, b := range entry.Value {
		f.data[int(pos.ValueStart)+i] = b
	}

	checkSumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSumBytes, entry.CheckSum())
	for i, b := range checkSumBytes {
		f.data[int(pos.ValueEnd-4)+i] = b
	}

	f.storeDataLen(totalLen)

	f.dataEnd.Store(uint64(totalLen))
	return pos, nil
}

// 每次写入后，把总长度（数据末尾位置，dataEnd）写入前8位
func (f *DataFile) storeDataLen(totalLen int64) {
	endData := make([]byte, 8)
	binary.BigEndian.PutUint64(endData, uint64(totalLen))
	for i := 0; i < 8; i++ {
		f.data[i] = endData[i]
	}
}

func (f *DataFile) Read(pos *DataPos) (*DataEntry, error) {
	if f.closed.Load() {
		return nil, fmt.Errorf("file :%s, closed", f.file.Name())
	}
	if pos == nil || pos.KeyStart > int64(dataMaxSize) ||
		pos.KeyEnd > int64(dataMaxSize) ||
		pos.KeyStart > pos.KeyEnd {
		return nil, fmt.Errorf("read data error, pos: %+v", pos)
	}

	key := make([]byte, pos.KeyEnd-pos.KeyStart)
	val := make([]byte, pos.ValueEnd-pos.ValueStart-4)

	copy(key, f.data[pos.KeyStart:pos.KeyEnd])
	copy(val, f.data[pos.ValueStart:pos.ValueEnd-4])
	dataEntry := &DataEntry{
		Key:   key,
		Value: val,
	}
	expected := binary.BigEndian.Uint32(f.data[pos.ValueEnd-4 : pos.ValueEnd])
	if dataEntry.CheckSum() != expected {
		return nil, fmt.Errorf("checksum error: %+v", pos)
	}
	return dataEntry, nil
}

func (f *DataFile) ReadKey(start, end uint64) []byte {
	if f.closed.Load() {
		return nil
	}
	if start > uint64(dataMaxSize) || end > uint64(dataMaxSize) || start >= end {
		return nil
	}
	key := make([]byte, end-start)
	copy(key, f.data[start:end])
	return key
}
