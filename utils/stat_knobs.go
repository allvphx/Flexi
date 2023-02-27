package utils

import (
	"FC/configs"
	"FC/network/detector"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Stat struct {
	mu       *sync.Mutex
	txnInfos []*Info
	beginTS  int
	endTS    int
}

func NewStat() *Stat {
	res := &Stat{
		txnInfos: make([]*Info, configs.MaxTID),
		mu:       &sync.Mutex{},
		beginTS:  0,
		endTS:    0,
	}
	return res
}

func (st *Stat) Append(info *Info) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.endTS++
	st.txnInfos[st.endTS] = info
}

func (st *Stat) Range() {
	st.mu.Lock()
	defer st.mu.Unlock()
}

func (st *Stat) Log() {
	st.mu.Lock()
	defer st.mu.Unlock()
	txnCnt, cross, success, fail, crossSuc, incorrectAss, contentedAbort, tryCnt := 0, 0, 0, 0, 0, 0, 0, 0
	latencySum, levelSum, s1, s2, s3 := 0, 0.0, time.Duration(0), time.Duration(0), time.Duration(0)
	latencies := make([]int, 0)
	//println(st.beginTS, st.endTS)
	for i := st.beginTS; i < st.endTS; i++ {
		if st.txnInfos[i] != nil {
			tmp := st.txnInfos[i]
			txnCnt++
			tryCnt += tmp.RetryCount
			incorrectAss += tmp.IncorrectAssumptionCnt
			contentedAbort += tmp.CCRetry
			if tmp.NumPart > 1 {
				cross++
				levelSum += float64(tmp.Level)
			}
			if tmp.Failure {
				fail++
			}
			if tmp.Latency > 0 {
				//latencySum += int(tmp.Latency)
				latencies = append(latencies, int(tmp.Latency))
			}
			if tmp.IsCommit {
				success++
				//				latencySum += int(tmp.Latency)
				//				latencies = append(latencies, int(tmp.Latency))
				if tmp.NumPart > 1 {
					latencySum += int(tmp.Latency)
					s1 += tmp.ST1
					s2 += tmp.ST2
					s3 += tmp.ST3
					crossSuc++
				}
			}
		}
	}
	base := configs.RunTestInterval
	if configs.TimeElapsedTest {
		base = 60
	}
	msg := "try_cnt:" + strconv.Itoa(tryCnt/base) + ";"
	msg += "txn_cnt:" + strconv.Itoa(txnCnt/base) + ";"
	msg += "dis_txn_cnt:" + strconv.Itoa(cross/base) + ";"
	msg += "client:" + strconv.Itoa(configs.ClientRoutineNumber) + ";"
	msg += "success_txn:" + strconv.Itoa(success/base) + ";"
	msg += "success_dis_txn:" + strconv.Itoa(crossSuc/base) + ";"
	msg += "crash_abort:" + strconv.Itoa(fail/base) + ";"
	msg += "level_abort:" + strconv.Itoa(incorrectAss/base) + ";"
	msg += "cc_abort:" + strconv.Itoa(contentedAbort/base) + ";"
	//msg := "try_cnt:" + strconv.Itoa(tryCnt) + ";"
	//msg += "txn_cnt:" + strconv.Itoa(txnCnt) + ";"
	//msg += "dis_txn_cnt:" + strconv.Itoa(cross) + ";"
	//msg += "client:" + strconv.Itoa(configs.ClientRoutineNumber) + ";"
	//msg += "success_txn:" + strconv.Itoa(success) + ";"
	//msg += "success_dis_txn:" + strconv.Itoa(crossSuc) + ";"
	//msg += "crash_abort:" + strconv.Itoa(fail) + ";"
	//msg += "level_abort:" + strconv.Itoa(incorrectAss) + ";"
	//msg += "cc_abort:" + strconv.Itoa(contentedAbort) + ";"
	sort.Ints(latencies)
	if len(latencies) > 0 {
		i := Min((len(latencies)*99+99)/100, len(latencies)-1)
		msg += "p99_latency:" + time.Duration(time.Duration(latencies[i]).Nanoseconds()).String() + ";"
		i = Min((len(latencies)*9+9)/10, len(latencies)-1)
		msg += "p90_latency:" + time.Duration(time.Duration(latencies[i]).Nanoseconds()).String() + ";"
		i = Min((len(latencies)+1)/2, len(latencies)-1)
		msg += "p50_latency:" + time.Duration(time.Duration(latencies[i]).Nanoseconds()).String() + ";"
		if crossSuc > 0 {
			msg += "ave_latency:" + time.Duration(time.Duration(latencySum/crossSuc).Nanoseconds()).String() + ";"
		} else {
			msg += "ave_latency:nil;"
		}
	} else {
		msg += "p99_latency:nil;"
		msg += "p90_latency:nil;"
		msg += "p50_latency:nil;"
		msg += "ave_latency:nil;"
	}
	if cross == 0 {
		msg += "avg_level:nil;"
	} else {
		msg += "avg_level:" + fmt.Sprintf("%f", levelSum/float64(cross)) + ";"
	}
	if crossSuc == 0 {
		msg += "avg_phase1:nil;"
		msg += "avg_phase2:nil;"
		msg += "avg_phase3:nil;"
	} else {
		msg += "avg_phase1:" + time.Duration(s1.Nanoseconds()/int64(crossSuc)).String() + ";"
		msg += "avg_phase2:" + time.Duration(s2.Nanoseconds()/int64(crossSuc)).String() + ";"
		msg += "avg_phase3:" + time.Duration(s3.Nanoseconds()/int64(crossSuc)).String() + ";"
	}
	fmt.Println(msg)
}

func (st *Stat) Clear() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.beginTS = 0
	st.endTS = 0
}

type Info struct {
	NumPart                int
	Failure                bool
	IncorrectAssumptionCnt int
	IncorrectAssumption    bool
	CCRetry                int
	Level                  int
	RetryCount             int
	IsCommit               bool
	Result                 *detector.KvResult
	Latency                time.Duration
	ST1                    time.Duration
	ST2                    time.Duration
	ST3                    time.Duration
}

func NewInfo(NPart int) *Info {
	res := &Info{
		NumPart: NPart,
		Failure: false, Level: -1, IsCommit: false, Latency: 0,
		ST1: 0, ST2: 0, ST3: 0, RetryCount: 0, IncorrectAssumptionCnt: 0, CCRetry: 0,
	}
	return res
}
