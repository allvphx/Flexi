package participant

import (
	"FC/configs"
	"bufio"
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Context records the statement context for Manager nodes.
type Context struct {
	mu           *sync.Mutex
	ctx          context.Context
	coordinator  string
	participants []string
	address      string
	wLatch       *sync.Mutex
	w            map[string]time.Duration // w(i) =  latency(coordinator,participant_i) + latency(participant_i, cur)
	cancel       context.CancelFunc
	queueLatch   *sync.Mutex
	msgQueue     [][]byte

	Manager *Manager // the participant manager

	done chan bool
	conn *Comm
}

var conLock = sync.Mutex{}
var config map[string]interface{}

func initData(stmt *Context, service string) {
	loadConfig(stmt, &config)
	stmt.msgQueue = make([][]byte, 0)
	configs.TPrintf("Load config finished")
	stmt.mu = &sync.Mutex{}
	stmt.wLatch = &sync.Mutex{}
	stmt.queueLatch = &sync.Mutex{}
	stmt.address = service
	storageSize := configs.NumberOfRecordsPerShard
	stmt.Manager = NewParticipantManager(stmt, storageSize)
}

func loadConfig(stmt *Context, config *map[string]interface{}) {
	conLock.Lock()
	defer conLock.Unlock()
	/* Read the config file and store it in 'config' variable */
	raw, err := os.ReadFile(configs.ConfigFileLocation)
	if err != nil {
		raw, err = os.ReadFile("." + configs.ConfigFileLocation)
	}
	configs.CheckError(err)

	err = json.Unmarshal(raw, &config)
	tmp, _ := ((*config)["participants"]).(map[string]interface{})
	stmt.participants = make([]string, 0)
	stmt.w = make(map[string]time.Duration)
	for i, p := range tmp {
		tp, err := strconv.Atoi(i)
		configs.CheckError(err)
		if tp <= configs.NumberOfShards {
			stmt.w[p.(string)] = 2 * configs.ExpBaseDelay
			stmt.participants = append(stmt.participants, p.(string))
		}
	}
	sort.Strings(stmt.participants)
	if len(configs.OuAddress) == 0 {
		configs.OuAddress = stmt.participants
	}
	tmp, _ = ((*config)["coordinators"]).(map[string]interface{})
	for _, p := range tmp {
		stmt.coordinator = p.(string)
	}
	stmt.done = make(chan bool, 1)
	configs.CheckError(err)
}

// Close the running participant process.
func (ctx *Context) Close() {
	configs.TPrintf("Close called!!! at " + ctx.address)
	ctx.done <- true
	ctx.cancel()
	ctx.conn.Stop()
}

func (ctx *Context) isTrace() bool {
	return configs.ServerAutoCrashEnabled && configs.Distribution != configs.Normal &&
		configs.Distribution != configs.Exponential &&
		configs.Distribution != configs.Plain
}

func (ctx *Context) isSingleInject() bool {
	return (!configs.LocalTest || ctx.address == configs.OuAddress[0]) && configs.ServerAutoCrashEnabled
}

func (ctx *Context) isSingleInjectNetworkFailure() bool {
	return (!configs.LocalTest || ctx.address == configs.OuAddress[0]) && configs.NetworkDisruptEnabled
}

func begin(stmt *Context, ch chan bool, service string) {
	configs.TPrintf("Initializing -- ")
	initData(stmt, service)
	configs.DPrintf(service)
	stmt.ctx, stmt.cancel = context.WithCancel(context.Background())
	stmt.conn = NewConns(stmt, service)

	configs.DPrintf("build finished for " + service)

	if stmt.isTrace() || stmt.isSingleInject() {
		stmt.injectCrashFailures()
	}
	if stmt.isSingleInjectNetworkFailure() { // inject network disrupts to all the participants.
		stmt.injectNetworkDisrupts()
	}
	ch <- true
	stmt.Run()
}

// Main the main function for a participant process.
func Main(addr string) {
	stmt := &Context{}
	ch := make(chan bool)
	go func() {
		<-ch
		if configs.Benchmark == "ycsb" {
			stmt.YCSBInit()
		} else {
			stmt.TPCInit()
		}
	}()
	begin(stmt, ch, addr)
}

func (ctx *Context) injectCrashFailures() {
	rand.Seed(42)
	var crashProbabilities = make([]float64, 0)
	if ctx.isTrace() {
		// traced failure occurrence from apps.
		//configs.Assert(configs.DistributionStartingPoint+(configs.RunTestInterval+59)/60 < 24*60,
		//	"starting point exceed distribution range")
		fileName := fmt.Sprintf("configs/crash_distributions/%s_dist.txt", configs.Distribution)
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text()) // Remove any extra spaces
			if num, err := strconv.ParseFloat(line, 64); err == nil {
				crashProbabilities = append(crashProbabilities, num)
			} else {
				fmt.Printf("Failed to parse float from line '%s': %v\n", line, err)
			}
		}

		scaleFactor := 1.0
		if err := scanner.Err(); err != nil {
			panic(err)
		}

		if err = file.Close(); err != nil {
			panic(err)
		}

		// simulated failure injections.
		configs.Assert(len(crashProbabilities) == 24*60, "It is not one day stats!")

		// find the hour that contains most failures.
		maxFailureSum := 0.0
		distributionStartingPoint := 0
		window := (configs.RunTestInterval + 59) / 60
		for i := 0; i < 24*60/window; i++ {
			curSum := 0.0
			for j := 0; j < window; j++ {
				curSum += crashProbabilities[i*window+j]
			}
			if curSum > maxFailureSum {
				maxFailureSum = curSum
				distributionStartingPoint = i
			}
		}

		go func() {
			time.Sleep(configs.WarmUpTime)
			curCrashed := false
			for i := 0; i < configs.RunTestInterval; i++ {
				prob := crashProbabilities[i/60+distributionStartingPoint] * scaleFactor
				if i%60 == 0 {
					configs.JPrint(prob)
				}
				crash := rand.Float64() < prob
				//fmt.Printf("The participant %v is in state %v at time %v(s) before atomic update, with %v parallel go routines\n",
				//	ctx.address,
				//	crash,
				//	i,
				//	runtime.NumGoroutine())
				if !curCrashed && crash {
					ctx.Manager.Break()
					curCrashed = true
				}
				if curCrashed && !crash {
					ctx.Manager.Recover()
					curCrashed = false
				}
				//fmt.Printf("The participant %v is in state %v at time %v(s), with %v parallel go routines\n",
				//	ctx.address,
				//	crash,
				//	i,
				//	runtime.NumGoroutine())
				time.Sleep(time.Second)
			}
		}()
	} else {
		if configs.ExpectedCrashTime == 0 {
			go func() {
				configs.Assert(configs.DelayStaticPreHeat < configs.WarmUpTime, "not enough warmup time")
				time.Sleep(configs.DelayStaticPreHeat)
				ctx.Manager.Break()
			}()
		} else {
			go func() {
				configs.Assert(configs.DelayStaticPreHeat < configs.WarmUpTime, "not enough warmup time")
				time.Sleep(configs.DelayStaticPreHeat)
				for {
					var nextFailureTime time.Duration
					if configs.Distribution == configs.Normal {
						nextFailureTime = time.Duration(math.Abs(rand.NormFloat64() * float64(configs.ExpectedCrashTime)))
					} else if configs.Distribution == configs.Exponential {
						nextFailureTime = time.Duration(math.Abs(rand.ExpFloat64() * float64(configs.ExpectedCrashTime)))
					} else if configs.Distribution == configs.Plain {
						nextFailureTime = configs.ExpectedCrashTime
					} else {
						panic("invalid distribution")
					}
					select {
					case <-ctx.ctx.Done():
						return
					case <-time.After(nextFailureTime):
						ctx.Manager.Break()
						time.Sleep(configs.CrashPeriod)
						ctx.Manager.Recover()
					}
				}
			}()
		}
	}
}

func (ctx *Context) injectNetworkDisrupts() {
	// for test, to simulate the jerky environments.
	if configs.ExpectedDelayTime == 0 {
		go func() {
			configs.Assert(configs.DelayStaticPreHeat < configs.WarmUpTime, "not enough warmup time")
			time.Sleep(configs.DelayStaticPreHeat)
			ctx.Manager.NetBreak()
		}()
	} else {
		go func() {
			configs.Assert(configs.DelayStaticPreHeat < configs.WarmUpTime, "not enough warmup time")
			time.Sleep(configs.DelayStaticPreHeat)
			for {
				var nextFailureTime time.Duration
				if configs.Distribution == configs.Normal {
					nextFailureTime = time.Duration(math.Abs(rand.NormFloat64() * float64(configs.ExpectedDelayTime)))
				} else if configs.Distribution == configs.Exponential {
					nextFailureTime = time.Duration(math.Abs(rand.ExpFloat64() * float64(configs.ExpectedDelayTime)))
				} else if configs.Distribution == configs.Plain {
					nextFailureTime = configs.ExpectedDelayTime
				} else {
					panic("invalid distribution")
				}
				select {
				case <-ctx.ctx.Done():
					return
				case <-time.After(nextFailureTime):
					ctx.Manager.NetBreak()
					time.Sleep(configs.DelayPeriod)
					ctx.Manager.NetRecover()
				}
			}
		}()
	}
}

func (ctx *Context) UpdateNetworkDelay(fromParticipant string, delay time.Duration) {
	ctx.wLatch.Lock()
	defer ctx.wLatch.Unlock()
	old, ok := ctx.w[fromParticipant]
	configs.Assert(ok, "the network timeout window is not initialized")
	if old == configs.CrashFailureTimeout {
		ctx.w[fromParticipant] = delay
	} else {
		// adjust the network timeout window.
		ctx.w[fromParticipant] = time.Duration(0.99*float64(old) + 0.01*float64(delay))
	}
}

func (ctx *Context) GetNetworkTimeOut(part []string) time.Duration {
	// max{ dis(C,j) + dis(j, aim) | j in ParticipantAddresses}
	ctx.wLatch.Lock()
	defer ctx.wLatch.Unlock()
	res := time.Duration(0)
	for _, p := range part {
		if res < ctx.w[p] {
			res = ctx.w[p]
		}
	}
	return time.Duration(float64(res) * configs.NetWorkDelayParameter)
}

func (ctx *Context) Run() {
	ctx.conn.Run()
}

func (ctx *Context) GetAddr() string {
	return ctx.address
}
