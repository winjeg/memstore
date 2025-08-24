package db

import (
	"gitlab.bitmartpro.com/spot-go/file-store/store"

	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func (s *FileStore) CurrentKV() (*KVUnit, string) {
	currentKV := s.currentKV
	if kv, ok := s.kvMap.Load(currentKV); ok {
		if result, ok := kv.(*KVUnit); ok {
			return result, currentKV
		}
	}
	return nil, ""
}

// Delete delete 在库满的时候仍需扩容
func (s *FileStore) Delete(k []byte) error {
	if s.closed.Load() {
		return ErrDBClosed
	}
	if se, ok := s.keyMap.Load(string(k)); ok && se != nil {
		if _, ok := se.(*StoreEntry); ok {
			version := s.currentVersion.Add(1)
			kvStore, current := s.CurrentKV()
			if kvStore != nil {
				if err := kvStore.Delete(k, version); err != nil {
					if errors.Is(err, store.ErrDatabaseFull) {
						return s.requireNewUnit(kvStore, k, nil, version, true)
					}
					return err
				} else {
					s.keyMap.Delete(string(k))
				}

			} else {
				return fmt.Errorf("kv %s not found", current)
			}
		}
	}
	return nil
}

// Set  更新状况： 如果是更新，内存会始终维护最新值，如果跟内存不一致，则丢弃
// 如果第一个文件满了，那么就去搞新的文件， 但是老的文件要立刻执行下sync操作
func (s *FileStore) setWithVersion(k, v []byte, version uint64) error {
	if s.closed.Load() {
		return ErrDBClosed
	}
	defer logCost(setDuration, time.Now(), s.monitor)
	kvStore, current := s.CurrentKV()
	pos, setErr := kvStore.Set(k, v, version)
	if setErr != nil {
		if errors.Is(setErr, store.ErrDatabaseFull) {
			return s.requireNewUnit(kvStore, k, v, version, false)
		} else {
			return setErr
		}
	} else {
		se := NewStoreEntry(current, pos.Start, pos.End, version)
		s.keyMap.Store(string(k), se)
		return nil
	}
}

func (s *FileStore) Set(k, v []byte) error {
	version := s.currentVersion.Add(1)
	return s.setWithVersion(k, v, version)
}

// 库满了，则新启一个新的库， 需要加锁
func (s *FileStore) requireNewUnit(oldUnit *KVUnit, k, v []byte, version uint64, isDelete bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed.Load() {
		return ErrDBClosed
	}
	// 再尝试一下写入，如果写入成功， 证明另外一个线程已经新起了一个文件
	unit, current := s.CurrentKV() // 此处的unit必须要拿最新的
	if isDelete {
		delErr := unit.Delete(k, version)
		if delErr == nil {
			s.keyMap.Delete(string(k))
		} else {
			if !errors.Is(delErr, store.ErrDatabaseFull) {
				return delErr
			}
		}
	} else {
		pos, setErr := unit.Set(k, v, version)
		// set 成功，直接返回, 失败则返回失败错误（非库满）
		if setErr == nil {
			se := NewStoreEntry(current, pos.Start, pos.End, version)
			s.keyMap.Store(string(k), se)
			return nil
		} else {
			if !errors.Is(setErr, store.ErrDatabaseFull) {
				return setErr
			}
		}
	}

	// 异步sync 当前的文件，当前文件写入不了了
	go func(kv *KVUnit) {
		if err := kv.Sync(); err != nil {
			fmt.Println(err.Error())
		}
	}(oldUnit)

	// 创建新的kv单元
	file := s.manifest.NewFile()
	kv := NewKVUnit(s.opts.StoreDir+"/"+file, s.logger)
	s.kvMap.Store(file, kv)
	s.currentKV = file
	if isDelete {
		if delErr := kv.Delete(k, version); delErr != nil {
			return delErr
		} else {
			s.keyMap.Delete(string(k))
		}
	} else {
		if p, e := kv.Set(k, v, version); e != nil {
			return e
		} else {
			se := NewStoreEntry(s.currentKV, p.Start, p.End, version)
			s.keyMap.Store(string(k), se)
		}
	}
	return nil
}

func (s *FileStore) Get(k []byte) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrDBClosed
	}
	defer logCost(getDuration, time.Now(), s.monitor)
	if v, ok := s.keyMap.Load(string(k)); ok && v != nil {
		// 从v 里面拿到一些内容，主要是meta的地址 file-index, 从哪个meta文件中去读取
		// 读取的位置信息， meta-start, meta-end 获取到meta，则检测meta中的信息是否是被删除了，或者过期了
		if se, ok := v.(*StoreEntry); ok {
			if load, ok := s.kvMap.Load(fmt.Sprintf("%08d", se.FileNumber)); ok {
				if kv, ok := load.(*KVUnit); ok {
					if d, err := kv.Get(se.MetaStart, se.MetaEnd); err != nil {
						if err != nil {
							if s.logger != nil {
								s.logger.Errorf("fileStore error get key: %s, err: %s", string(k), string(err.Error()))
							}
						}
						return nil, err
					} else {
						return d, nil
					}
				}
			}
		}
	}
	return nil, ErrKeyNotFound
}

// Range 遍历DB集合
func (s *FileStore) Range(f func(k string) bool) {
	if s.monitor != nil {
		s.monitor.Inc(scanCount, "type", "key")
	}
	s.keyMap.Range(func(key, _ any) bool {
		return f(key.(string))
	})
}

func (s *FileStore) RangeKV(f func(k, v string) bool) {
	if s.monitor != nil {
		s.monitor.Inc(scanCount, "type", "key-value")
	}
	s.keyMap.Range(func(key, v any) bool {
		return f(key.(string), v.(string))
	})
}

func (s *FileStore) GetKeyMap() *sync.Map {
	return &s.keyMap
}

func (s *FileStore) DebugInfo(key string) string {
	debugInfo := "debug info:\n"
	debugInfo += s.currentKV + "\n"
	debugInfo += fmt.Sprintf("%+v\n", s.manifest.Files)
	if v, ok := s.keyMap.Load(key); ok {
		debugInfo += fmt.Sprintf("key: %s exsit\n", key)
		if v != nil {
			debugInfo += fmt.Sprintf("entry: %+v\n", v)
			if se, ok := v.(*StoreEntry); ok {
				if load, ok := s.kvMap.Load(fmt.Sprintf("%08d", se.FileNumber)); ok {
					debugInfo += fmt.Sprintf("kv: %08d Exsit\n", se.FileNumber)
					if kv, ok := load.(*KVUnit); ok {
						debugInfo += fmt.Sprintf("meta start: %d, meta end: %d\n", se.MetaStart, se.MetaEnd)
						entryData, metaErr := kv.GetMeta(se.MetaStart, se.MetaEnd)
						if metaErr != nil {
							debugInfo += fmt.Sprintf("get meta err: %s\n", metaErr.Error())
						} else {
							debugInfo += fmt.Sprintf("%+v\n", entryData)
							meta := &store.EntryMeta{}
							meta = meta.FromBytes(entryData)
							if meta == nil {
								debugInfo += "get meta failed!"
							}
						}
					}
				}
			}
		}
	}
	return debugInfo
}

// Backup 将当前FileStore中的所有key-value对备份到指定文件
func (s *FileStore) Backup(file *os.File) error {
	if s.closed.Load() {
		return ErrDBClosed
	}

	// 创建一个编码器来写入文件
	encoder := gob.NewEncoder(file)

	// 使用通道来流式处理数据
	type kvPair struct {
		Key   []byte
		Value []byte
	}
	kvChan := make(chan kvPair, 1000) // 使用缓冲通道
	errChan := make(chan error, 1)
	done := make(chan struct{})

	// 启动一个goroutine来处理数据写入
	go func() {
		defer close(errChan)
		for pair := range kvChan {
			if err := encoder.Encode(pair); err != nil {
				errChan <- fmt.Errorf("failed to encode data: %v", err)
				return
			}
		}
		done <- struct{}{}
	}()

	// 遍历所有key-value对
	var backupErr error
	s.keyMap.Range(func(key, value interface{}) bool {
		select {
		case err := <-errChan:
			backupErr = err
			return false
		default:
			if se, ok := value.(*StoreEntry); ok {
				if kv, ok := s.kvMap.Load(fmt.Sprintf("%08d", se.FileNumber)); ok {
					if kvUnit, ok := kv.(*KVUnit); ok {
						if data, err := kvUnit.Get(se.MetaStart, se.MetaEnd); err == nil {
							kvChan <- kvPair{
								Key:   []byte(key.(string)),
								Value: data,
							}
						}
					}
				}
			}
			return true
		}
	})

	close(kvChan)

	// 等待写入完成或错误发生
	select {
	case <-done:
		// 正常完成
	case err := <-errChan:
		backupErr = err
	}

	if backupErr != nil {
		return backupErr
	}

	return nil
}

// LoadFromFile 从备份文件中恢复数据到FileStore
func (s *FileStore) LoadFromFile(file *os.File) error {
	if s.closed.Load() {
		return ErrDBClosed
	}

	// 创建一个解码器来读取文件
	decoder := gob.NewDecoder(file)

	// 清空当前数据
	s.keyMap = sync.Map{}

	// 使用通道来流式处理数据
	type kvPair struct {
		Key   []byte
		Value []byte
	}
	kvChan := make(chan kvPair, 1000) // 使用缓冲通道
	errChan := make(chan error, 1)

	// 启动一个goroutine来读取数据
	go func() {
		defer close(kvChan)
		for {
			var pair kvPair
			if err := decoder.Decode(&pair); err != nil {
				if err == io.EOF {
					return
				}
				errChan <- fmt.Errorf("failed to decode data: %v", err)
				return
			}
			kvChan <- pair
		}
	}()

	// 处理读取到的数据
	for pair := range kvChan {
		if err := s.Set(pair.Key, pair.Value); err != nil {
			return fmt.Errorf("failed to restore key %s: %v", string(pair.Key), err)
		}
	}

	// 检查是否有错误发生
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}
