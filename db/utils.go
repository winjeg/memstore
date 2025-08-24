package db

import (
	"fmt"
	"gitlab.bitmartpro.com/spot-go/file-store/store"
	"strings"
	"time"
)

func logError(logger store.Logger, msg string, err error) {
	if err != nil {
		if logger != nil {
			logger.Errorf("file_store error %s, %s", msg, err.Error())
		} else {
			fmt.Printf("file_store error %s, %s \n", msg, err.Error())
		}
	}
}

func logCost(name string, t time.Time, m Monitor) {
	if m != nil {
		nanos := time.Since(t).Nanoseconds()
		m.Summary(name, float64(nanos))
	}
}

// Contains to judge if the target is contained in arr
func contains(arr []string, target string) bool {
	if arr == nil {
		return false
	}
	for _, v := range arr {
		if strings.EqualFold(target, v) {
			return true
		}
	}
	return false
}
