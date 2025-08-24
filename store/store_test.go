package store

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
	"unsafe"
)

const orderData = `{SyncType:order UserId:4897559 DelegationType:0 Sequence: 
Orders:[{ID:992836206744246017 
Type:0 BizType:1 BizSubType:0 ClientOrderID: MerchantID:0 Side:1 State:1 UserID:4897559 
SymbolID:3411 Price:0.191 ExpectAmount:117.9043 ReceiptAmount:0 ExpectSize:617.3 ReceiptSize:0
ExpectFee:0 ReceiptFee:0 PositiveFee:0 FeeCoinId:17 FeeCoinName:USDT ForceClose:0 
CreateTime:1742177649423 UpdateTime:0 EndTime:0 PrevID:0 PrevFlag:0 Version:1 TrxComplete:0
Source:{"chnl_src":"1"} FeeInfo:{"fee":"0","maker":"-0.0001","taker":"0.0001","coinId":4053,
"deduction":"0","calcAmount":"117.9043","marketCoinId":17} BargainInfo:{} MarginOrderMode:0 
PlanType:0 DelegationType:0 Extra: CancelState:0} {ID:992836206744246018 Type:0 BizType:1
BizSubType:0 ClientOrderID: MerchantID:0 Side:1 State:1 UserID:4897559 SymbolID:3411 
Price:0.199 ExpectAmount:15.721 ReceiptAmount:0 ExpectSize:79 ReceiptSize:0 ExpectFee:0 
ReceiptFee:0 PositiveFee:0 FeeCoinId:17 FeeCoinName:USDT ForceClose:0 CreateTime:1742177649423
UpdateTime:0 EndTime:0 PrevID:0 PrevFlag:0 Version:1 TrxComplete:0 Source:{"chnl_src":"1"} 
FeeInfo:{"fee":"0","maker":"-0.0001","taker":"0.0001","coinId":4053,"deduction":"0",
"calcAmount":"15.721","marketCoinId":17} BargainInfo:{} MarginOrderMode:0 PlanType:0 
DelegationType:0 Extra: CancelState:0}] OrderDetails:[] SelfDetails:[] Balances:[{CoinId:4053
BalanceTotal:256173.761 BalanceFrozen:192071.1 UpdateTime:1742177649423 UserId:4897559 
Version:756033613 CreateTime:1735132531948 FeeFrozen:0}] BalanceLogs:[] IsolatedBalances:[] 
IsolatedBalanceLogs:[]}`

func TestMapFile(t *testing.T) {
	slog.Default()
	dataFile := NewDataFile("demo.store", nil)
	metaData := NewMetaFile("demo.meta", nil)
	start := time.Now()
	writeCount := 0
	for i := 0; i < 10_000_000; i++ {
		key := fmt.Sprintf("key_%d", i)
		if pos, err := dataFile.WriteData(&DataEntry{
			Key:   []byte(key),
			Value: []byte(orderData),
		}); err != nil {
			fmt.Println(err.Error())
			break
		} else {
			if _, err := metaData.WriteMeta(&EntryMeta{
				KeyStart:   uint64(pos.KeyStart),
				KeyEnd:     uint64(pos.KeyEnd),
				ValueStart: uint64(pos.ValueStart),
				ValueEnd:   uint64(pos.ValueEnd),
				Meta:       0,
				ExpireTime: 0,
				Version:    1,
			}); err != nil {
				fmt.Println(err.Error())
				break
			} else {
				writeCount++
			}
		}
	}
	fmt.Printf("write data time cost:%d\twrite count:%d\n", time.Since(start).Microseconds(), writeCount)
	start2 := time.Now()
	count := 0
	metaData.LoadAll(func(bytes []byte, start, end uint64) {
		meta := &EntryMeta{}
		meta.FromBytes(bytes)
		_, _ = dataFile.Read(&DataPos{
			KeyStart:   int64(meta.KeyStart),
			KeyEnd:     int64(meta.KeyEnd),
			ValueStart: int64(meta.ValueStart),
			ValueEnd:   int64(meta.ValueEnd),
		})
		count++
	})
	fmt.Printf("load data time cost:%d, count:%d\n", time.Since(start2).Microseconds(), count)
	dataFile.Close()
	metaData.Close()

	os.Remove("demo.store")
	os.Remove("demo.meta")
}

func TestEntryMeta(t *testing.T) {
	fmt.Println("EntryMeta 实际大小:", unsafe.Sizeof(EntryMeta{}))
}

func TestRead(t *testing.T) {
	metaData := NewMetaFile("demo.meta", nil)
	metaData.Unmap()
	metaData.Read(63<<20, 63<<20+entryLen)
}
