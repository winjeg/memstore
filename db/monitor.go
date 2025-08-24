package db

const (
	fileStorePrefix = "file_store_"
	setDuration     = fileStorePrefix + "set_duration"
	getDuration     = fileStorePrefix + "get_duration"
	scanCount       = fileStorePrefix + "scan"
	compactNum      = fileStorePrefix + "compact"
	syncDiskNum     = fileStorePrefix + "sync_disk"
)

type Monitor interface {
	Count(name string, count float64, tags ...string)
	Timer(name string, v float64, tags ...string)
	TimerWithBuckets(name string, v float64, buckets []float64, tags ...string)
	Gauge(name string, v float64, tags ...string)
	Summary(name string, v float64, tags ...string)
	Inc(name string, tags ...string)
	Dec(name string, tags ...string)
}
