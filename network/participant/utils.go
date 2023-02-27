package participant

import (
	"FC/configs"
	"FC/storage"
	"fmt"
	"github.com/magiconair/properties/assert"
	"strconv"
	"testing"
)

var address []string

func TestKit() []*Context {
	address = make([]string, 0)
	for i := 0; i < configs.NumberOfShards; i++ {
		address = append(address, fmt.Sprintf("127.0.0.1:60%02d", i+1))
	}

	stmts := make([]*Context, configs.NumberOfShards)
	ch := make(chan bool)

	for i := 0; i < configs.NumberOfShards; i++ {
		var args = []string{"*", "*", address[i], "*"}
		stmts[i] = &Context{}
		go begin(stmts[i], ch, args[2])
		<-ch
	}

	if configs.EnableReplication {
		for i := 0; i < configs.NumberOfShards; i++ {
			for j := 0; j < configs.NumberOfReplicas; j++ {
				// the current nodes contains the data for shard [i] [i-1] ... [i-R+1]
				replica := stmts[i].participants[(i-j+configs.NumberOfShards)%configs.NumberOfShards]
				stmts[i].Manager.Shards[replica] = storage.Testkit(replica)
			}
		}
	} else {
		for i := 0; i < configs.NumberOfShards; i++ {
			stmts[i].Manager.Shards[stmts[i].address] = storage.Testkit(stmts[i].address)
		}
	}
	return stmts
}

func (ctx *Context) YCSBInit() {
	if configs.EnableReplication {
		panic("not supported yet")
		//for j := 0; j < configs.NumberOfReplicas; j++ {
		//	// the current nodes contains the data for shard [i] [i-1] ... [i-R+1]
		//	replica := ctx.participants[(i-j+configs.NumberOfShards)%configs.NumberOfShards]
		//	ctx.Manager.Shards[replica] = storage.YCSBStorageKit(replica)
		//}
	} else {
		ctx.Manager.Shards[ctx.address] = storage.YCSBStorageKit(ctx.address)
	}
}

func (ctx *Context) TPCInit() {
	if configs.EnableReplication {
		panic("not supported yet")
		//for j := 0; j < configs.NumberOfReplicas; j++ {
		//	// the current nodes contains the data for shard [i] [i-1] ... [i-R+1]
		//	replica := ctx.participants[(i-j+configs.NumberOfShards)%configs.NumberOfShards]
		//	ctx.Manager.Shards[replica] = storage.YCSBStorageKit(replica)
		//}
	} else {
		ctx.Manager.Shards[ctx.address] = storage.TPCCStorageKit(ctx.address)
	}
}

func YCSBParticipantKit() []*Context {
	address = make([]string, 0)
	for i := 0; i < configs.NumberOfShards; i++ {
		address = append(address, fmt.Sprintf("127.0.0.1:60%02d", i+1))
	}

	stmts := make([]*Context, configs.NumberOfShards)
	ch := make(chan bool)

	for i := 0; i < configs.NumberOfShards; i++ {
		var args = []string{"*", "*", address[i], "*"}
		stmts[i] = &Context{}
		go begin(stmts[i], ch, args[2])
		<-ch
	}

	if configs.EnableReplication {
		for i := 0; i < configs.NumberOfShards; i++ {
			for j := 0; j < configs.NumberOfReplicas; j++ {
				// the current nodes contains the data for shard [i] [i-1] ... [i-R+1]
				replica := stmts[i].participants[(i-j+configs.NumberOfShards)%configs.NumberOfShards]
				stmts[i].Manager.Shards[replica] = storage.YCSBStorageKit(replica)
			}
		}
	} else {
		for i := 0; i < configs.NumberOfShards; i++ {
			stmts[i].Manager.Shards[stmts[i].address] = storage.YCSBStorageKit(stmts[i].address)
		}
	}
	return stmts
}

func TPCCParticipantKit() []*Context {
	address = make([]string, 0)
	for i := 0; i < configs.NumberOfShards; i++ {
		address = append(address, fmt.Sprintf("127.0.0.1:60%02d", i+1))
	}

	stmts := make([]*Context, configs.NumberOfShards)
	ch := make(chan bool)

	for i := 0; i < configs.NumberOfShards; i++ {
		var args = []string{"*", "*", address[i], "*"}
		stmts[i] = &Context{}
		go begin(stmts[i], ch, args[2])
		<-ch
	}

	if configs.EnableReplication {
		for i := 0; i < configs.NumberOfShards; i++ {
			for j := 0; j < configs.NumberOfReplicas; j++ {
				// the current nodes contains the data for shard [i] [i-1] ... [i-R+1]
				replica := stmts[i].participants[(i-j+configs.NumberOfShards)%configs.NumberOfShards]
				stmts[i].Manager.Shards[replica] = storage.TPCCStorageKit(replica)
			}
		}
	} else {
		for i := 0; i < configs.NumberOfShards; i++ {
			stmts[i].Manager.Shards[stmts[i].address] = storage.TPCCStorageKit(stmts[i].address)
		}
	}
	return stmts
}

func LoadIntValue(value interface{}) int {
	//fmt.Println(reflect.TypeOf(value))
	switch v := value.(type) {
	case float64:
		return int(v)
	case uint32:
		return int(v)
	case int32:
		return int(v)
	case uint:
		return int(v)
	case float32:
		return int(v)
	case int:
		return v
	case string:
		val, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		return val
	default:
		panic("invalid case")
	}
}

func CheckVal(t *testing.T, coh *Manager, ans []string) {
	//configs.JPrint("start check")
	for i := 0; i < len(ans); i++ {
		v, ok := coh.Shards[coh.stmt.address].Read("MAIN", uint64(i))
		for !ok {
			v, ok = coh.Shards[coh.stmt.address].Read("MAIN", uint64(i))
		}
		//configs.JPrint(ans[i] + " - " + strconv.Itoa(v.GetAttribute(0).(int)))
		configs.JPrint(v)
		assert.Equal(t, ok && ans[i] == strconv.Itoa(LoadIntValue(v.GetAttribute(0))),
			true, "unexpected result")
	}
}
