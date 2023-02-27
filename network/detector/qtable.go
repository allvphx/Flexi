package detector

import (
	"FC/configs"
	rl "FC/network/detector/.detector"
	"context"
	"google.golang.org/grpc"
	"time"
)

type QTLearner struct {
	level Level
	react int
}

func Stop() {
}

func (c QTLearner) Init(cid string, i int) {
	c.level = NoCFNoNF
	c.react = NoAction
}

func (c QTLearner) Reset(level Level, cid string) {
	conn, err := grpc.Dial("localhost:5003", grpc.WithInsecure())
	defer conn.Close()
	configs.CheckError(err)
	reset := rl.NewResetClient(conn)
	re := GetReward()
	lev := int32(level)
	_, err = reset.Reset(context.Background(), &rl.Info{Cid: &cid, Level: &lev, Reward: &re})
	if err != nil {
		panic(err)
	}
}

func (c QTLearner) Trans(level Level, cid string) int {
	// interact with the reinforcement learning model.
	conn, err := grpc.Dial("localhost:5003", grpc.WithInsecure())
	defer conn.Close()
	configs.CheckError(err)
	act := rl.NewActionClient(conn)
	re := GetReward()
	lev := int32(level)
	reply, err := act.Action(context.Background(), &rl.Info{Cid: &cid, Level: &lev, Reward: &re})
	if err != nil {
		panic(err)
	}
	return int(reply.GetAction())
}

func (c QTLearner) Send(level Level, cid string, failure bool) {
	if level != c.level || failure {
		// reset the RL model state.
		c.Reset(level, cid)
		c.level = level
		c.react = DelayOneStep
	} else {
		// transition
		c.react = c.Trans(level, cid)
		if c.react == DownTransAction {
			// back to initial Level
			c.level = NoCFNoNF
		}
	}
}

func (c QTLearner) Action(cid string) int {
	for {
		rec := c.react
		if rec == -1 {
			time.Sleep(5 * time.Millisecond)
			continue
		} else {
			c.react = -1
			return rec
		}
	}
}
