package db

import (
	"time"
)

// RunSyncTasks 同步磁盘的任务， 只同步当前文件，历史文件理应同步完毕了
func (s *FileStore) RunSyncTasks() {
	ticker := time.NewTicker(time.Second * time.Duration(s.opts.SyncInterval))
	for {
		select {
		case <-ticker.C:
			s.Sync()
		case <-s.close:
			if s.logger != nil {
				s.logger.Infoln("RunCompactTasks - task stopped")
			}
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

// RunCompactTasks 压缩是根据文件个数进行压缩
// 1. 达到指定数量后从最老的文件开始压缩
// 2. 当一个文件压缩完毕删除之前，需要执行sync
func (s *FileStore) RunCompactTasks() {
	for {
		select {
		case <-s.close:
			if s.logger != nil {
				s.logger.Infoln("RunCompactTasks - task stopped")
			}
			return
		default:
			s.Compact()
			time.Sleep(time.Second * 1)
		}
	}
}

func (s *FileStore) RunStatistics() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			s.Statistics()
		case <-s.close:
			if s.logger != nil {
				s.logger.Infoln("RunCompactTasks - task stopped")
			}
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

func (s *FileStore) Statistics() {
	if s.monitor == nil {
		//dataSizeTotal := len(s.manifest.Files) * s.opts.MaxDataSize
		//s.monitor.Gauge(dataTotal, float64(dataSizeTotal))
		//s.monitor.Gauge(fileTotal, float64(len(s.manifest.Files)))
	}
}
