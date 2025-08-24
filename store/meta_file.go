/// meta 和KEY,VALUE放在不同文件里，文件都使用内存映射
/// 每次提交KEY VALUE对的时候，先操作文件，再操作内存，当最后一个东西操作成功的时候返回成功，否则返回失败。
/// 先操作 KEY，VALUE， 再操作内存， 最后再操作Meta， Meta操作成功视为写入完成

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

const metaMaxSize = 64 << 20

var ErrDatabaseFull = fmt.Errorf("error database is full")

const defaultMemMapSize = 8 * (1 << 20) // 映射的内存大小为默认8MB
type MetaFile struct {
	file    *os.File
	data    *[metaMaxSize]byte
	dataRef []byte
	metaEnd atomic.Int64 // meta结束的位置， 开始的位置为0
	lock    sync.Mutex
	closed  atomic.Bool
	logger  Logger
}

func NewMetaFile(file string, logger Logger) *MetaFile {
	f, _ := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	fkv := &MetaFile{file: f, lock: sync.Mutex{}, closed: atomic.Bool{}, logger: logger}
	if err := fkv.init(); err != nil {
		if fkv.logger != nil {
			fkv.logger.Errorf("error init meta file: %s", err.Error())
		}
		return nil
	}
	return fkv
}

func (f *MetaFile) init() error {
	if err := f.mmap(); err != nil {
		return err
	}
	metaEnd := binary.BigEndian.Uint64(f.data[:int(8)])
	f.metaEnd.Store(int64(metaEnd))
	return nil
}

func (f *MetaFile) mmap() error {
	// 1. 按页大小对齐映射长度
	pageSize := os.Getpagesize()
	alignedSize := (metaMaxSize + pageSize - 1) &^ (pageSize - 1)
	b, err := amap.MMap(f.file, true, int64(alignedSize))
	if err != nil {
		return err
	}
	addr := uintptr(unsafe.Pointer(&b[0]))
	f.dataRef = b
	f.data = (*[metaMaxSize]byte)(unsafe.Pointer(addr))
	return nil
}

func (f *MetaFile) grow(size int64) error {
	if info, _ := f.file.Stat(); info.Size() >= size {
		return nil
	}
	if err := f.file.Truncate(size); err != nil {
		return err
	}
	return nil
}

func (f *MetaFile) Unmap() error {
	if err := amap.Munmap(f.dataRef); err != nil {
		return err
	}
	f.data = nil
	f.dataRef = nil
	return nil
}

func (f *MetaFile) Close() error {
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

func (f *MetaFile) Sync() error {
	if f.closed.Load() {
		return fmt.Errorf("file :%s, closed", f.file.Name())
	}
	return amap.Msync(f.dataRef)
}

func (f *MetaFile) WriteMeta(meta *EntryMeta) (*Pos, error) {
	if f.closed.Load() {
		return nil, fmt.Errorf("file :%s, closed", f.file.Name())
	}
	if meta == nil {
		return nil, errors.New("meta store nil")
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	metaEnd := binary.BigEndian.Uint64(f.data[:int(8)])
	if metaEnd == 0 {
		metaEnd = 8
	}
	data := meta.ToBytes()
	totalLen := int64(metaEnd) + int64(len(data))
	if totalLen >= int64(metaMaxSize) {
		return nil, ErrDatabaseFull
	}
	// 空间足够，写入数据
	for i, b := range data {
		f.data[int(metaEnd)+i] = b
	}
	f.writeTotalLen(totalLen)
	return &Pos{
		Start: metaEnd,
		End:   uint64(totalLen),
	}, nil
}

func (f *MetaFile) writeTotalLen(totalLen int64) {
	metaData := make([]byte, 8)
	binary.BigEndian.PutUint64(metaData, uint64(totalLen))
	for i := 0; i < 8; i++ {
		f.data[i] = metaData[i]
	}
	f.metaEnd.Store(totalLen)
}

// LoadAll 此处如果有锁则会影响其他写入
// 无锁对于正在写入的文件会只遍历一部分
// 对于已经不变动的文件没问题，对于变动的文件，则如当时的一个快照一样
func (f *MetaFile) LoadAll(read func([]byte, uint64, uint64)) {
	//f.lock.Lock()
	//defer f.lock.Unlock()
	if f.closed.Load() {
		return
	}
	metaEnd := binary.BigEndian.Uint64(f.data[:int(8)])
	for i := 8; i < int(metaEnd); i += entryLen {
		if f.closed.Load() {
			return
		}
		metaBytes := make([]byte, entryLen)
		copy(metaBytes, f.data[i:i+entryLen])
		read(metaBytes, uint64(i), uint64(i+entryLen))
	}
}

func (f *MetaFile) Read(start uint64, end uint64) ([]byte, error) {
	defer func() {
		if err := recover(); err != nil {
			if f.logger != nil {
				f.logger.Errorf("file: %s, start: %d, end: %d", f.file.Name(), start, end)
			}
		}
	}()
	if f.closed.Load() {
		return nil, fmt.Errorf("file :%s, closed", f.file.Name())
	}
	if start > uint64(metaMaxSize) || end > uint64(metaMaxSize) {
		return nil, errors.New("out of meta read range")
	}
	if end-start != entryLen {
		return nil, fmt.Errorf("wrong meta size, start :%d, end: %d", start, end)
	}
	if len(f.data) >= int(start) && len(f.data) >= int(end) {
		metaBytes := make([]byte, entryLen)
		copy(metaBytes, f.data[start:end])
		return metaBytes, nil
	}
	return nil, errors.New("meta not found")
}
