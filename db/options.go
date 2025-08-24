package db

import "gitlab.bitmartpro.com/spot-go/file-store/store"

const (
	defaultCompactNum    = 8 // 默认超过8个文件才开始压缩，即数据量300万左右才开始压缩
	defaultCompactThread = 3 // 默认最多3个压缩线程
	defaultSyncInterval  = 5 // 默认5秒执行sync磁盘一次
)

type Options struct {
	StoreDir      string // 数据库的目录
	CompactNum    int    // 达到多少文件之后才开始compact
	CompactThread int    // compact的线程数
	SyncInterval  int    // 磁盘同步的间隔， 单位秒
	Logger        store.Logger
}

// DefaultOptions size 参数一旦设定则不可调整， 找到一个最合适的使用即可
func DefaultOptions(path string, log store.Logger) Options {
	return Options{
		StoreDir:      path,
		CompactNum:    defaultCompactNum,
		CompactThread: defaultCompactThread,
		SyncInterval:  defaultSyncInterval,
		Logger:        log,
	}
}

func (opt Options) WithCompactNum(num int) Options {
	opt.CompactNum = num
	return opt
}

func (opt Options) WithCompactThreadNum(num int) Options {
	opt.CompactThread = num
	return opt
}
func (opt Options) WithSyncInterval(sec int) Options {
	opt.SyncInterval = sec
	return opt
}
