package detector

import (
	"FC/configs"
	"sync"
	"time"
)

type Learner interface {
	Send(level Level, cid string, failure bool)
	Action(cid string) int
	Init(cid string, i int)
}

var Learners []Learner

var lockStat = &sync.Mutex{}
var CommittedTxn = 0
var lastTime = time.Now()

func CountThroughput() {
	lockStat.Lock()
	defer lockStat.Unlock()
	CommittedTxn++
}

func GetReward() float32 {
	lockStat.Lock()
	defer lockStat.Unlock()
	res := float32(CommittedTxn) / float32(time.Since(lastTime).Seconds())
	CommittedTxn = 0
	lastTime = time.Now()
	return res
}

func InitLearners() {
	Learners = make([]Learner, configs.NumberOfShards*2)
	switch configs.TunerAlgorithm {
	case configs.FixedParam:
		for i := 0; i < configs.NumberOfShards; i++ {
			Learners[i] = SimpleLearner{}
			Learners[i].Init(configs.OuAddress[i%configs.NumberOfShards], i)
		}
	case configs.GridSearch:
		for i := 0; i < configs.NumberOfShards; i++ {
			Learners[i] = GridLearner{}
			Learners[i].Init(configs.OuAddress[i%configs.NumberOfShards], i)
		}
	case configs.RLTuning:
		for i := 0; i < configs.NumberOfShards; i++ {
			Learners[i] = QTLearner{}
			Learners[i].Init(configs.OuAddress[i%configs.NumberOfShards], i)
		}
	case configs.SimulatedAnnealing:
		for i := 0; i < configs.NumberOfShards; i++ {
			Learners[i] = SALearner{}
			Learners[i].Init(configs.OuAddress[i%configs.NumberOfShards], i)
		}
	default:
		panic("invalid learning algorithm")
	}
}

func Send(level Level, cid string, failure bool) {
	i := -1
	for j := 0; j < len(configs.OuAddress); j++ {
		if configs.OuAddress[j] == cid {
			i = j
		}
	}
	if level == CFNF {
		i += 3
	}
	Learners[i].Send(level, cid, failure)
}

func Action(level Level, cid string) int {
	i := -1
	for j := 0; j < len(configs.OuAddress); j++ {
		if configs.OuAddress[j] == cid {
			i = j
		}
	}
	if level == CFNF {
		i += 3
	}
	return Learners[i].Action(cid)
}
