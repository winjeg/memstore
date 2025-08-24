package db

import (
	"gitlab.bitmartpro.com/spot-go/file-store/store"

	"fmt"
	"os"
	"testing"
)

func TestManifest_Load(t *testing.T) {
	_ = os.Remove("demo.manifest")
	mf := NewManifest("demo.manifest")
	loadErr := mf.Load()
	store.AssertEqual(t, loadErr, nil)
	store.AssertEqual(t, loadErr, nil)
	fileName := mf.NewFile()
	store.AssertEqual(t, fileName, fmt.Sprintf("%08d", 2))
	mf.NewFile()
	mf.Delete(fileName)
	mf.Load()
	store.AssertEqual(t, len(mf.Files), 2)
	_ = os.Remove("demo.manifest")

}
