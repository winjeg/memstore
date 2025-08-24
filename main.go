package main

import (
	"fmt"
	"gitlab.bitmartpro.com/spot-go/file-store/db"
	"time"
)

func readyTestStore() *db.FileStore {
	options := db.DefaultOptions("/Users/winjeg/data_files", nil)
	if db, err := db.Open(options); err != nil {
		panic(err)
	} else {
		return db
	}
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

func main() {
	start := time.Now()
	db := readyTestStore()
	defer func() {
		fmt.Println("closed by main thread.")
		db.Close()
	}()
	fmt.Printf("db load cost: %d, count:%s\n", time.Since(start).Microseconds(), db.Info())
	fmt.Println(db.Info())
	for i := 0; i < 10_000_000; i++ {
		k := []byte(fmt.Sprintf("k_%09d", i))
		v := []byte(v)
		db.Set(k, v)
	}
	count := 0
	db.Range(func(_ string) bool {
		count++
		return true
	})
	fmt.Printf("total Keys : %d\n", count)
	select {}
}
