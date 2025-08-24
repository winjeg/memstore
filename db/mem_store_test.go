package db

import (
	"gitlab.bitmartpro.com/spot-go/file-store/store"

	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

const testDataDir = "./data/TestFileStore_Load"

func readyTestStore(dir string) *FileStore {
	//_ = os.RemoveAll(dir)
	options := DefaultOptions(dir, nil)
	options.WithSyncInterval(10)
	options = options.WithCompactNum(2)
	if db, err := Open(options); err != nil {
		panic(err)
	} else {
		return db
	}
}

func clean() {
	_ = os.RemoveAll("./data")
}

func TestFileStore_Close(t *testing.T) {
	db := readyTestStore(testDataDir)
	defer db.Close()
	time.Sleep(1 * time.Second)
	clean()
}

func TestFileStore_Backup(t *testing.T) {
	db := readyTestStore(testDataDir)
	for i := 0; i < 50000; i++ {
		_ = db.Set([]byte(fmt.Sprintf("k_%d", i)), []byte(v))
	}
	_ = os.MkdirAll("./data/backup", os.ModePerm)
	backupFile, _ := os.Create("./data/backup/backup.bak")
	err := db.Backup(backupFile)
	if err != nil {
		t.Errorf("Backup failed: %v", err)
	}
	db.Close()
	time.Sleep(1 * time.Second)
	clean()
}

func TestFileStore_LoadFromFile(t *testing.T) {
	backupFile, err := os.Open("./data/backup/backup.bak")
	if err != nil {
		t.Errorf("Failed to open backup file: %v", err)
		return
	}
	defer backupFile.Close()

	db := readyTestStore(testDataDir)
	err = db.LoadFromFile(backupFile)
	if err != nil {
		t.Errorf("LoadFromFile failed: %v", err)
		return
	}

	for i := 0; i < 1500000; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("k_%d", i)))
		if err != nil {
			t.Errorf("Get failed: %v", err)
			return
		}
		store.AssertEqual(t, fmt.Sprintf("v_%d", i), string(v))
	}

	time.Sleep(1 * time.Second)
	clean()
}

func TestStoreEntry_ToBytes(t *testing.T) {
	metaByte := []byte{0, 0, 0, 0, 3, 91, 94, 112, 0, 0, 0, 0, 3, 91, 94, 133, 0, 0, 0, 0, 3, 91, 94, 133, 0, 0, 0, 0, 3, 91, 95, 70, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 25, 27, 135, 125, 0, 0, 0, 0, 207, 10, 163, 133}
	meta := &store.EntryMeta{}
	meta.FromBytes(metaByte)
	t.Logf("meta: %+v", meta)
	t.Logf("meta: %+v", len([]byte("1065648375011615232")))
	//KeyStart:33121 KeyEnd:33169 ValueStart:33169 ValueEnd:33192 ExpireTime:9223372036854775807 Version:401668610 Meta:0 CheckSum:0
	//KeyStart:56319600 KeyEnd:56319621 ValueStart:56319621 ValueEnd:56319814 ExpireTime:9223372036854775807 Version:421234557 Meta:0 CheckSum:0}
}

func TestFileStore_Load(t *testing.T) {
	db := readyTestStore(testDataDir)
	db.Range(func(key string) bool {
		return true
	})
	defer db.Close()
	if v0, err0 := db.Get([]byte("k1")); err0 != nil {
		store.AssertEqual(t, err0, ErrKeyNotFound)
	} else {
		store.AssertEqual(t, "v4", string(v0))
	}
	db.Set([]byte("k1"), []byte("v1"))
	if v, err := db.Get([]byte("k1")); err != nil {
		fmt.Println(err.Error())
	} else {
		store.AssertEqual(t, string(v), "v1")
	}
	db.Delete([]byte("k1"))
	v2, err2 := db.Get([]byte("k1"))
	store.AssertEqual(t, err2, ErrKeyNotFound)
	store.AssertNil(t, v2)
	db.Set([]byte("k1"), []byte("v4"))
	v4, err4 := db.Get([]byte("k1"))
	store.AssertNil(t, err4)
	store.AssertEqual(t, string(v4), "v4")
	fmt.Println(db.DebugInfo("k1"))

	time.Sleep(1 * time.Second)
	clean()
}

func TestFileStore_Compact(t *testing.T) {
	start := time.Now()
	db := readyTestStore(testDataDir)
	defer db.Close()
	fmt.Printf("db load cost: %d, count:%d\n", time.Since(start).Microseconds(), db.totalKeyCount)
	fmt.Println(db.Info())
	count := 0
	db.Range(func(_ string) bool {
		count++
		return true
	})
	fmt.Printf("total Keys : %d\n", count)
	for i := 0; i < 5_000_000; i++ {
		k := []byte(fmt.Sprintf("k_%09d", i))
		v := []byte(v)
		db.Set(k, v)
		db.Delete(k)
		//if i == 1357893 {
		//	panic("error ..... ")
		//}
	}
	db.Compact()

	time.Sleep(1 * time.Second)
	clean()
}

const v = `
 order is not open state, order:{"id":1006227707187127811,"type":0,
"bizType":1,"bizSubType":0,"clientOrderId":"BMBMOOXUSDT1742975845002510",
"merchantId":"0","side":0,"state":1,"userId":5233058,"symbolId":3375,
"price":"0.152","expectAmount":"10.04872","receiptAmount":"0",
"expectSize":"66.11","receiptSize":"0","expectFee":"0.004019488",
"receiptFee":"0","positiveFee":"0","feeCoinId":17,"feeCoinName":"USDT",
"forceClose":0,"createTime":1742975845051,"updateTime":1742975870054,
"endTime":0,"prevId":0,"prevFlag":0,"version":2,"trxComplete":0,
"source":"{\"chnl_src\":\"1\"}",
"feeInfo":"{\"fee\":\"0.004019488\",\"maker\":\"-0.00001\",
\"taker\":\"0.0004\",\"coinId\":4022,\"deduction\":\"0\",\"calcAmount\":
\"10.04872\",\"marketCoinId\":17}","bargainInfo":"{}","marginOrderMode":0,
"planType":0,"delegationType":0,"extra":"","cancelState":1}
`

func BenchmarkFileStore_Get(b *testing.B) {
	db := readyTestStore(testDataDir)
	defer db.Close()
	v := []byte(v)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("k_%09d", i))
		db.Set(k, v)
	}
	b.ReportAllocs()

	time.Sleep(1 * time.Second)
	clean()
}

func TestFileStore_Set(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(4)
	m := make(map[int]*FileStore, 4)
	for i := 0; i < 4; i++ {
		go func(i int, wg *sync.WaitGroup) {
			db := readyTestStore(fmt.Sprintf("%s-%d", testDataDir, i))
			m[i] = db
			for i := 0; i < 5_000_000; i++ {
				k := []byte(fmt.Sprintf("k_%09d", i))
				v := []byte(v)
				db.Set(k, v)
				db.Delete(k)
			}
			db.Compact()
			wg.Done()
		}(i, wg)
	}
	wg.Wait()
	defer func() {
		for _, v := range m {
			v.Close()
		}
	}()

	time.Sleep(1 * time.Second)
	clean()

}
func TestFileStore_Load2x(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	m := make(map[int]*FileStore, 4)
	start := time.Now()
	for i := 0; i < 1; i++ {
		go func(i int, wg *sync.WaitGroup) {
			db := readyTestStore(fmt.Sprintf("%s-%d", testDataDir, i))
			m[i] = db
			wg.Done()
		}(i, wg)
	}
	wg.Wait()
	fmt.Printf("total cost: %d ms\n", time.Since(start).Milliseconds())
	defer func() {
		for _, v := range m {
			v.Close()
		}
	}()

	time.Sleep(1 * time.Second)
	clean()
}
