package db

/// file_store, 一个基于内存文件映射的，不支持事务的本地 KEY-VALUE数据存储
/// 纯内存化的设计方案，所有操作都基于内存，同时尽可能的持久化到磁盘上。

import (
	"gitlab.bitmartpro.com/spot-go/file-store/store"

	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const manifestFileName = "MANIFEST"

const endMark = "store_closed"

// Open  打开这个数据库， 加载数据到内存
func Open(opt Options) (*FileStore, error) {
	if len(opt.StoreDir) == 0 {
		return nil, errors.New("directory must be specified")
	}
	if err := createDirIfNotExists(opt.StoreDir); err != nil {
		return nil, err
	}

	manifest := NewManifest(opt.StoreDir + "/" + manifestFileName)
	if err := manifest.Load(); err != nil {
		return nil, err
	}

	if !contains(manifest.Files, endMark) && len(manifest.Files) != 1 {
		//return nil, errors.New("database closed unexpectedly")
		msg := fmt.Sprintf("database closed unexpectedly, %+v, opts:%+v", manifest.Files, opt)
		logError(opt.Logger, msg, errors.New("database closed unexpectedly"))
	}

	logError(opt.Logger, "database start up remove endMark error", manifest.Delete(endMark))

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	fileStore := &FileStore{
		opts:       &opt,
		manifest:   manifest,
		compactors: 1,
		keyMap:     sync.Map{},
		lock:       sync.Mutex{},
		kvMap:      sync.Map{},
		close:      make(chan int),
		closed:     atomic.Bool{},
		logger:     opt.Logger,
	}

	if err := fileStore.Load(); err != nil {
		return nil, err
	}

	go fileStore.RunSyncTasks()
	go fileStore.RunCompactTasks()
	go fileStore.RunStatistics()

	go func(ch chan os.Signal) {
		for {
			select {
			case <-quit:
				if !fileStore.closed.Load() {
					fileStore.Close()
				}
			}
		}
	}(quit)
	return fileStore, nil
}
func CheckCloseMark(path string) bool {
	if len(path) == 0 {
		return false
	}
	if err := createDirIfNotExists(path); err != nil {
		return false
	}

	manifest := NewManifest(path + "/" + manifestFileName)
	if err := manifest.Load(); err != nil {
		return false
	}

	if !contains(manifest.Files, endMark) && len(manifest.Files) != 1 {
		return false
	}
	return true
}

type FileStore struct {
	opts           *Options
	manifest       *Manifest  // 新值永远对应manifest 对应的地方
	compactors     int        // how many compactors
	keyMap         sync.Map   //  all the valid key value， key就是正常的key，value则是 manifest 对应的 文件编号-metaFile的地址
	delMap         sync.Map   // 删除的map，只在load时期有用
	lock           sync.Mutex // 在一些操作的时候要短暂获取此锁以免引起并发问题
	kvMap          sync.Map   // Key 为文件名或者文件索引， Value为对应的基础KV存储
	currentKV      string
	currentVersion atomic.Uint64
	close          chan int
	totalKeyCount  int64
	closed         atomic.Bool
	monitor        Monitor
	logger         store.Logger
}

func (s *FileStore) WithMonitor(m Monitor) {
	if s.monitor == nil && m != nil {
		s.monitor = m
	}
}

func (s *FileStore) WithLogger(l store.Logger) {
	if s.logger != nil && l != nil {
		s.logger = l
	}
}

// Sync 进行磁盘同步，建议每几秒一次
// 与mySQL提交间隔类似,控制刷盘的间隔
// 程序退出时候也应该执行一次
// sync对于已经 固化下来的文件（写满的）不再执行，只执行正在写入的文件
// 对于已经写满的文件，会由set写满的时候触发sync
func (s *FileStore) Sync() {
	if kv, ok := s.kvMap.Load(s.currentKV); ok {
		if v, ok := kv.(*KVUnit); ok {
			if s.closed.Load() {
				return
			}
			err := v.Sync()
			logError(s.logger, fmt.Sprintf("sync disk error, opt:%+v", s.opts), err)
			if s.monitor != nil {
				if err != nil {
					s.monitor.Count(syncDiskNum, 1, "type", "error")
				} else {
					s.monitor.Count(syncDiskNum, 1, "type", "ok")
				}
			}
		}
	}
}

// Close 关闭之后，写一个结束标记，表明数据均已完成落盘
func (s *FileStore) Close() {
	if s.closed.Load() {
		return
	}

	if s.closed.CompareAndSwap(false, true) {
		close(s.close)
		// 睡眠1ms让其他协程处理自己的事情
		time.Sleep(time.Millisecond)
		s.kvMap.Range(func(key, value any) bool {
			if v, ok := value.(*KVUnit); ok {
				go func(unit *KVUnit) {
					logError(s.logger, "close kvUnit error", unit.Close())
				}(v)
			}
			return true
		})
	}
	logError(s.logger, "error writing end mark", s.manifest.write(endMark))
}

// Compact 做compaction的时候， 从最老的文件开始读取，读取到的东西检测有没有删除，如没有删除
// 如何确定有没有删除: 看内存里面， 如果当前内存里面没有，则判定为删除
// num 为合并的线程数
func (s *FileStore) Compact() {
	// 遍历所有的文件， 可以多线程遍历也可以单线程
	// 需要特殊的转移API， 转移到最新的文件中即可
	remainNum := len(s.manifest.Files) - s.opts.CompactNum
	if remainNum <= 0 {
		return
	}
	num := min(remainNum, s.opts.CompactThread)
	wg := sync.WaitGroup{}
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func(x int, group *sync.WaitGroup) {
			s.doCompact(s.manifest.Files[x])
			group.Done()
		}(i, &wg)
	}
	wg.Wait()
}

// TODO 对于同一个文件只能有一个协程来compact，用程序保障一下
// 实际做压缩的动作， 包括搜刮有用的key，写入新的key
// 删除老旧的文件，以及同步当前的kv
func (s *FileStore) doCompact(file string) {
	if s.closed.Load() {
		return
	}
	if unit, ok := s.kvMap.Load(file); ok {
		if kv, ok := unit.(*KVUnit); ok {
			if s.logger != nil {
				s.logger.Infof("doCompact -compacting file: %s", file)
			}

			kv.Range(func(k, v []byte, meta *store.EntryMeta, start, end uint64) {
				if s.closed.Load() {
					return
				}
				if s.monitor != nil {
					s.monitor.Count(compactNum, 1, "type", "total")
				}
				// 有用的保留下来，支持多线程并发写入， 没用的清理掉
				if se, ok := s.keyMap.Load(string(k)); ok && se != nil {
					// 此处不应该出错，如果出错了， 一般为磁盘错误
					if e, ok := se.(*StoreEntry); ok && meta.Version >= e.Version {
						if s.monitor != nil {
							s.monitor.Count(compactNum, 1, "type", "keep")
						}
						if err := s.setWithVersion(k, v, meta.Version); err != nil {
							if s.monitor != nil {
								s.monitor.Count(compactNum, 1, "type", "error")
							}
							logError(s.logger, fmt.Sprintf("compact move error k:%s, v:%s", string(k), string(v)), err)
						}
					}
				}
			})
		}
		// 遍历完成，迁移完成, 如果没有错误，那么就删除原来的文件
		s.Sync()
		logError(s.logger, "remove current key value unit", s.RemoveKVUnit(file))
	}
}

// Load meta file里面，去load的时候，始终只load最新的meta，要全局比对一下
// load 完成之前，不能使用此store， 否则会引起不可预知的后果
func (s *FileStore) Load() error {
	maxVersion := uint64(0)
	ts := time.Now()
	for i, fileName := range s.manifest.Files {
		start, count := time.Now(), 0
		kv := NewKVUnit(s.opts.StoreDir+"/"+fileName, s.logger)
		if s.logger != nil {
			s.logger.Infof("load file: %s, total:%d", fileName, len(s.manifest.Files))
		}

		s.kvMap.Store(fileName, kv)

		if i == len(s.manifest.Files)-1 {
			s.currentKV = fileName
		}
		kv.RangeKey(func(k []byte, meta *store.EntryMeta, start, end uint64) {
			if meta.Version > maxVersion {
				maxVersion = meta.Version
			}
			s.totalKeyCount++
			count++
			// 如果是删除的，则load未完成之前先不要删除，找个map存一下， load完成之后再删除
			//fmt.Println(string(k), "\t", string(_v), "\t", meta.Version, "\t", meta.Meta, "\t", start, "\t", end)
			if dse, ok := s.delMap.Load(string(k)); !ok {
				if se, ok := s.keyMap.Load(string(k)); ok && se != nil {
					// 如果已经有了，呢么就只存最新版本
					if e, ok := se.(*StoreEntry); ok {
						if e.Version < meta.Version {
							storeEntry := NewStoreEntry(fileName, start, end, meta.Version)
							if meta.IsDeleted() {
								s.delMap.Store(string(k), storeEntry)
								s.keyMap.Delete(string(k))
							} else {
								s.keyMap.Store(string(k), storeEntry)
							}
						}
					}
				} else {
					// 如果不存在，那么就存当前版本
					storeEntry := NewStoreEntry(fileName, start, end, meta.Version)
					if !meta.IsDeleted() {
						s.keyMap.Store(string(k), storeEntry)
					} else {
						s.delMap.Store(string(k), storeEntry)
					}
				}
			} else {
				// 存在删掉的key， 已经删掉的key同样比对版本号
				if dse != nil {
					if se, ok := dse.(*StoreEntry); ok {
						if meta.Version > se.Version {
							storeEntry := NewStoreEntry(fileName, start, end, meta.Version)
							if !meta.IsDeleted() {
								s.keyMap.Store(string(k), storeEntry)
								s.delMap.Delete(string(k))
							} else {
								s.delMap.Store(string(k), storeEntry)
							}
						}
					}
				}
			}
		})
		fmt.Printf("--- loading kv: %s cost: %d ms, key count: %d \n", fileName, time.Since(start).Milliseconds(), count)
	}
	fmt.Printf("loading %d keys, total cost: %d\n", s.totalKeyCount, time.Since(ts).Milliseconds())
	s.currentVersion.Store(maxVersion)
	s.delMap = sync.Map{} // 清除删除的map
	return nil
}

// RemoveKVUnit 实现逻辑
func (s *FileStore) RemoveKVUnit(file string) error {
	if s.closed.Load() {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	// 先从清单中移除
	if err := s.manifest.Delete(file); err != nil {
		return err
	}
	// 再移除实际的内存映射文件
	if kv, ok := s.kvMap.Load(file); ok {
		if v, ok := kv.(*KVUnit); ok {
			if err := v.Remove(); err != nil {
				logError(s.logger, "removeKVUnit - error remove", err)
				return err
			} else {
				s.kvMap.Delete(file)
			}
		}
	}
	return nil
}

func (s *FileStore) Info() string {
	return fmt.Sprintf("db path: %s\nkeys loaded: %d\ncurrent kv: %s\ncurrentVersion:%d\n",
		s.opts.StoreDir, s.totalKeyCount, s.currentKV, s.currentVersion.Load())
}

func createDirIfNotExists(dir string) error {
	// 检查目录是否存在
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// 创建目录（包括所有必要的父目录）
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	} else if err != nil {
		// 如果不是 "目录不存在" 的错误，则可能是其他问题，如权限问题
		return fmt.Errorf("error checking directory: %w", err)
	}
	return nil
}
