package benchmark

import (
	"FC/configs"
	"FC/network/coordinator"
	"FC/network/participant"
	"FC/storage"
	"FC/utils"
	"fmt"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"
)

type YCSBStmt struct {
	stat         *utils.Stat
	coordinator  *coordinator.Context
	participants []*participant.Context
	stop         int32
}

type YCSBClient struct {
	md   int
	from *YCSBStmt
	r    *rand.Rand
	zip  *generator.Zipfian
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (c *YCSBClient) generateTxnKVPairs(parts []string, TID uint64) []storage.TXOpt {
	kvTransaction := make([]storage.TXOpt, 0)
	var j int
	var val string
	val = randSeq(5)
	md := c.md
	isDistributedTxn := rand.Intn(100) < configs.CrossShardTXNPercentage

	if configs.TransactionLength == 0 {
		// special case used to test transaction varying participants: all participants one operation, used to test the system scalability.
		var key uint64
		for j = 0; j < configs.NumberOfShards; j++ {
			key = uint64(c.zip.Next(c.r))
			/* Access the key from different partitions */
			configs.TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": " + strconv.Itoa(j) + "[" + strconv.FormatUint(key, 10) + "] = " + val)
			isRead := rand.Float64() < configs.ReadPercentage
			if isRead {
				kvTransaction = append(kvTransaction, storage.TXOpt{
					Type:  storage.ReadOpt,
					Shard: parts[j],
					Table: "YCSB_MAIN",
					Key:   key,
				})
			} else {
				kvTransaction = append(kvTransaction, storage.TXOpt{
					Type:  storage.UpdateOpt,
					Shard: parts[j],
					Table: "YCSB_MAIN",
					Key:   key,
					Value: storage.WrapYCSBTestValue(val),
				})
			}
		}
	} else {
		var key uint64
		for i := 0; i < configs.TransactionLength; i++ {
			/* Code for contentious key selection */
			j = random(0, configs.ShardsPerTransaction)
			if i < configs.ShardsPerTransaction && isDistributedTxn {
				/* Ensure txn spans all partitions */
				j = (i + md) % configs.NumberOfShards
				val = configs.ZeroValue
			} else if !isDistributedTxn {
				j = md % configs.NumberOfShards
			} else {
				j = (j + md) % configs.NumberOfShards
			}
			key = uint64(c.zip.Next(c.r))
			/* Access the key from different partitions */
			configs.TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": " + strconv.Itoa(j) + "[" + strconv.FormatUint(key, 10) + "] = " + val)

			isRead := rand.Float64() < configs.ReadPercentage
			if isRead {
				kvTransaction = append(kvTransaction, storage.TXOpt{
					Type:  storage.ReadOpt,
					Shard: parts[j],
					Table: "YCSB_MAIN",
					Key:   key,
				})
			} else {
				kvTransaction = append(kvTransaction, storage.TXOpt{
					Type:  storage.UpdateOpt,
					Shard: parts[j],
					Table: "YCSB_MAIN",
					Key:   key,
					Value: storage.WrapYCSBTestValue(val),
				})
			}
		}
	}
	return kvTransaction
}

func (c *YCSBClient) performTransactions(TID uint64, participants []string, txn *coordinator.TX, stats *utils.Stat) (bool, *coordinator.TX) {
	defer configs.TimeTrack(time.Now(), "performTransactions", TID)
	if txn == nil {
		kvData := c.generateTxnKVPairs(participants, TID)
		exist := make(map[string]bool)
		parts := make([]string, 0)
		for _, v := range kvData {
			sd := v.Shard
			if exist[sd] == false {
				exist[sd] = true
				parts = append(parts, sd)
			}
		}
		txn = coordinator.NewTX(TID, parts, c.from.coordinator.Manager)
		txn.OptList = kvData
	} else {
		txn.TxnID = TID
	}
	info := utils.NewInfo(len(txn.Participants))
	configs.DPrintf("TXN%v: Start on client %v", TID, c.md)
	c.from.coordinator.Manager.SubmitTxn(txn, configs.SelectedACP, info)
	stats.Append(info)
	if info.IsCommit {
		configs.DPrintf("TXN%v: Commit on client %v", TID, c.md)
	} else {
		configs.DPrintf("TXN%v: Abort on client %v", TID, c.md)
	}
	return info.IsCommit, txn
}

func (stmt *YCSBStmt) Stopped() bool {
	return atomic.LoadInt32(&stmt.stop) != 0
}

func (stmt *YCSBStmt) startYCSBClient(seed int, md int) {
	client := YCSBClient{md: md, from: stmt}

	client.r = rand.New(rand.NewSource(int64(seed)*11 + 31))
	client.zip = generator.NewZipfianWithRange(0, int64(configs.NumberOfRecordsPerShard-2), configs.YCSBDataSkewness)
	for !stmt.Stopped() {
		TID := utils.GetTxnID()
		client.performTransactions(TID, configs.OuAddress, nil, stmt.stat)
	}
}

func (stmt *YCSBStmt) Stop() {
	stmt.coordinator.Close()
	atomic.StoreInt32(&stmt.stop, 1)
	if stmt.participants == nil {
		return
	}
	for _, v := range stmt.participants {
		v.Close()
	}
}

func (stmt *YCSBStmt) YCSBTest() {
	if configs.SelectedACP == configs.GPAC {
		configs.EnableReplication = true
	}
	var highestCPUUsage float64
	var highestBandwidthUsage float64
	var highestMemUsage float64
	var basicCPUUsage float64
	var basicMemUsage float64
	var prevIOCounters []net.IOCountersStat
	var err error
	if configs.ShowOverallCPUNetworkUsage {
		highestCPUUsage = 0.0
		highestBandwidthUsage = 0.0
		highestMemUsage = 0.0
		prevIOCounters, err = net.IOCounters(true)
		if err != nil {
			fmt.Println("Error getting net IO counters:", err)
			return
		}
		percent, err := cpu.Percent(time.Second, false)
		if err != nil {
			fmt.Println("Error getting CPU percent:", err)
			return
		}
		basicCPUUsage = percent[0]
		v, err := mem.VirtualMemory()
		if err != nil {
			fmt.Println("Error getting virtual memory info:", err)
			return
		}

		// Convert to GB for easier reading
		basicMemUsage = float64(v.Used) / float64(1024*1024)
	}
	if configs.LocalTest {
		stmt.coordinator, stmt.participants = coordinator.YCSBTestKit()
	} else {
		stmt.coordinator = coordinator.NormalKit(configs.CoordinatorServerAddress)
		stmt.participants = nil
	}
	stmt.stat = utils.NewStat()
	rand.Seed(1234)
	for i := 0; i < configs.ClientRoutineNumber; i++ {
		go stmt.startYCSBClient(i*11+13, i)
	}
	configs.TPrintf("All clients Started")
	if configs.TimeElapsedTest {
		stmt.stat.Clear()
		for i := 0; i < configs.RunTestInterval/60; i++ {
			time.Sleep(time.Minute)
			stmt.stat.Log()
			stmt.stat.Clear()
		}
	} else {
		time.Sleep(configs.WarmUpTime)
		stmt.stat.Clear()
		for i := 0; i < configs.RunTestInterval; i++ {
			if configs.ShowOverallCPUNetworkUsage {
				percent, err := cpu.Percent(time.Second, false)
				if err != nil {
					fmt.Println("Error getting CPU percent:", err)
					return
				}
				if percent[0] > highestCPUUsage {
					highestCPUUsage = percent[0]
				}

				ioCounters, err := net.IOCounters(true)
				if err != nil {
					fmt.Println("Error getting net IO counters:", err)
					return
				}

				for index, counter := range ioCounters {
					bytesSent := float64(counter.BytesSent - prevIOCounters[index].BytesSent)
					bytesRecv := float64(counter.BytesRecv - prevIOCounters[index].BytesRecv)
					totalBytes := bytesSent + bytesRecv

					// Convert to Mbps (assuming 1 second interval)
					bandwidthUsage := totalBytes * 8 / 1000000

					if bandwidthUsage > highestBandwidthUsage {
						highestBandwidthUsage = bandwidthUsage
					}
				}
				prevIOCounters = ioCounters

				v, err := mem.VirtualMemory()
				if err != nil {
					fmt.Println("Error getting virtual memory info:", err)
					return
				}

				// Convert to GB for easier reading
				usedGB := float64(v.Used) / float64(1024*1024)
				if usedGB > highestMemUsage {
					highestMemUsage = usedGB
				}
			}
			time.Sleep(time.Second)
		}
		stmt.stat.Log()
		if configs.ShowOverallCPUNetworkUsage {
			fmt.Printf("Highest: %.2f Mbps, %.2f%%, %.2f MB\n",
				highestBandwidthUsage, highestCPUUsage-basicCPUUsage, highestMemUsage-basicMemUsage)
		}
		stmt.stat.Clear()
	}
}
