package db

import (
	"os"

	"gitlab.bitmartpro.com/spot-go/file-store/store"

	"fmt"
	"testing"
)

func TestNewKVUnit(t *testing.T) {
	kv := NewKVUnit("demo", nil)
	if _, err := kv.Set([]byte("k3"), []byte("v2"), 1); err != nil {
		t.FailNow()
	}
	kv.Range(func(k, v []byte, meta *store.EntryMeta, start, end uint64) {
		fmt.Printf("k:%s\tv:%s\n", string(k), string(v))
	})
	kv.Sync()
	kv.Close()
	if err := os.Remove("./demo.data"); err != nil {
		println(err.Error())
	}
	os.Remove("./demo.meta")

}
