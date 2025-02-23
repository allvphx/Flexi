package coordinator

import (
	"FC/configs"
	"FC/storage"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func genLocalTX(ma *Manager, shard string, n, rang int, txnID uint64, readPer float64) *TX {
	configs.TimeTrack(time.Now(), "txnAccess in participant", txnID)
	tx := NewTX(txnID, []string{shard}, ma)
	for i := 0; i < n; i++ {
		if rand.Float64() < readPer {
			tx.AddRead("MAIN", shard, uint64(rand.Intn(rang)+1))
		} else {
			tx.AddUpdate("MAIN", shard, uint64(rand.Intn(rang)+1), storage.WrapTestValue(rand.Intn(10000)))
		}
	}
	return tx
}

func TestCoordinatorSideLocalTxN(t *testing.T) {
	configs.StorageType = configs.MongoDB
	makeLocal()
	defer recLocal()
	ca, cohorts := TestKit()
	rand.Seed(123)
	var latencySum int64 = 0
	for con := 1; con <= 32; con *= 2 {
		st := time.Now()
		suc := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					tx := genLocalTX(ca.Manager, address[0], 5, configs.NumberOfRecordsPerShard-1, uint64(i+configs.SpeedTestBatchPerThread*pos), 0.5)
					txnBeginTime := time.Now()
					if ca.Manager.SingleSubmit(tx, nil) {
						atomic.AddInt64(&latencySum, int64(time.Since(txnBeginTime)))
						atomic.AddInt32(&suc, 1)
					}
				}
				done <- true
			}(ch, c)
		}
		for i := 0; i < con; i++ {
			<-ch
		}
		totTime := time.Duration(latencySum)
		bas := time.Since(st).Seconds()
		fmt.Printf("with %v concurrent threads, %.2f local transactions executed in one second, %.2f success, %.2f (ms) average latency\n",
			con, float64(configs.SpeedTestBatchPerThread)*float64(con)/bas, float64(suc)/bas, totTime.Seconds()*1000/float64(suc))
	}
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func genDistributedTX(ma *Manager, n, rang int, txnID uint64, readPer float64) *TX {
	configs.TimeTrack(time.Now(), "txnAccess in participant", txnID)
	tx := NewTX(txnID, address[:configs.Min(n, 3)], ma)
	for i := 0; i < n; i++ {
		if rand.Float64() < readPer {
			tx.AddRead("MAIN", address[i%3], uint64(rand.Intn(rang)+1))
		} else {
			tx.AddUpdate("MAIN", address[i%3], uint64(rand.Intn(rang)+1), storage.WrapTestValue(rand.Intn(10000)))
		}
	}
	return tx
}

func TestCoordinatorSide2PC(t *testing.T) {
	configs.StorageType = configs.MongoDB
	makeLocal()
	defer recLocal()
	ca, cohorts := TestKit()
	rand.Seed(time.Now().UnixNano())
	var latencySum int64 = 0
	for con := 1; con <= 16; con *= 2 {
		st := time.Now()
		suc := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					tx := genDistributedTX(ca.Manager, 5, configs.NumberOfRecordsPerShard-1, uint64(i+configs.SpeedTestBatchPerThread*pos), 1)
					txnBeginTime := time.Now()
					if ca.Manager.TwoPCSubmit(tx, nil) {
						atomic.AddInt64(&latencySum, int64(time.Since(txnBeginTime)))
						atomic.AddInt32(&suc, 1)
					}
				}
				done <- true
			}(ch, c)
		}
		for i := 0; i < con; i++ {
			<-ch
		}
		totTime := time.Duration(latencySum)
		bas := time.Since(st).Seconds()
		fmt.Printf("with %v concurrent threads, %.2f local transactions executed in one second, %.2f success, %.2f (ms) average latency\n",
			con, float64(configs.SpeedTestBatchPerThread)*float64(con)/bas, float64(suc)/bas, totTime.Seconds()*1000/float64(suc))
		if con == 8 {
			time.Sleep(2 * time.Second)
		}
	}
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestCoordinatorSide3PC(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, cohorts := TestKit()
	rand.Seed(time.Now().UnixNano())
	var latencySum int64 = 0
	for con := 1; con <= 16; con *= 2 {
		st := time.Now()
		suc := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					tx := genDistributedTX(ca.Manager, 5, configs.NumberOfRecordsPerShard-1, uint64(i+configs.SpeedTestBatchPerThread*pos), 1)
					txnBeginTime := time.Now()
					if ca.Manager.ThreePCSubmit(tx, nil) {
						atomic.AddInt64(&latencySum, int64(time.Since(txnBeginTime)))
						atomic.AddInt32(&suc, 1)
					}
				}
				done <- true
			}(ch, c)
		}
		for i := 0; i < con; i++ {
			<-ch
		}
		totTime := time.Duration(latencySum)
		bas := time.Since(st).Seconds()
		fmt.Printf("with %v concurrent threads, %.2f local transactions executed in one second, %.2f success, %.2f (ms) average latency\n",
			con, float64(configs.SpeedTestBatchPerThread)*float64(con)/bas, float64(suc)/bas, totTime.Seconds()*1000/float64(suc))
		if con == 8 {
			time.Sleep(2 * time.Second)
		}
	}
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestCoordinatorSideEC(t *testing.T) {
	// crashes when running this, why?
	makeLocal()
	defer recLocal()
	ca, cohorts := TestKit()
	rand.Seed(233)
	var latencySum int64 = 0
	for con := 1; con <= 16; con *= 2 {
		st := time.Now()
		suc := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					tx := genDistributedTX(ca.Manager, 5, configs.NumberOfRecordsPerShard-1, uint64(i+configs.SpeedTestBatchPerThread*pos), 1)
					txnBeginTime := time.Now()
					if ca.Manager.EasySubmit(tx, nil) {
						atomic.AddInt64(&latencySum, int64(time.Since(txnBeginTime)))
						atomic.AddInt32(&suc, 1)
					}
				}
				done <- true
			}(ch, c)
		}
		for i := 0; i < con; i++ {
			<-ch
		}
		totTime := time.Duration(latencySum)
		bas := time.Since(st).Seconds()
		fmt.Printf("with %v concurrent threads, %.2f local transactions executed in one second, %.2f success, %.2f (ms) average latency\n",
			con, float64(configs.SpeedTestBatchPerThread)*float64(con)/bas, float64(suc)/bas, totTime.Seconds()*1000/float64(suc))
		if con == 8 {
			time.Sleep(2 * time.Second)
		}
	}
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestCoordinatorSideFC(t *testing.T) {
	// crashes when running this, why?
	makeLocal()
	defer recLocal()
	ca, cohorts := TestKit()
	rand.Seed(233)
	var latencySum int64 = 0
	for con := 1; con <= 16; con *= 2 {
		st := time.Now()
		suc := int32(0)
		//total := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					tx := genDistributedTX(ca.Manager, 5, configs.NumberOfRecordsPerShard-1, uint64(i+configs.SpeedTestBatchPerThread*pos), 1)
					txnBeginTime := time.Now()
					if ca.Manager.FCffSubmit(tx, nil, 0) {
						atomic.AddInt64(&latencySum, int64(time.Since(txnBeginTime)))
						atomic.AddInt32(&suc, 1)
					}
				}
				done <- true
			}(ch, c)
		}
		for i := 0; i < con; i++ {
			<-ch
		}
		totTime := time.Duration(latencySum)
		bas := time.Since(st).Seconds()
		fmt.Printf("with %v concurrent threads, %.2f local transactions executed in one second, %.2f success, %.2f (ms) average latency\n",
			con, float64(configs.SpeedTestBatchPerThread)*float64(con)/bas, float64(suc)/bas, totTime.Seconds()*1000/float64(suc))
		if con == 8 {
			time.Sleep(2 * time.Second)
		}
	}
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}
