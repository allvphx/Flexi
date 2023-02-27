package detector

import (
	"FC/configs"
)

type SimpleLearner struct {
	level   Level
	counter int
	react   int
}

const (
	NoAction        = -1
	DownTransAction = 0
	DelayOneStep    = 1
	// for the action number > 1, we wait for action steps before next interaction with the learner.
)

func (c SimpleLearner) Init(cid string, i int) {
	c.level = NoCFNoNF
	c.counter = 1
	c.react = NoAction
}

func (c SimpleLearner) Send(level Level, cid string, failure bool) {
	if level == 1 {
		c.react = DelayOneStep
		return
	}
	if level != c.level || failure {
		// reset when there is a failure encountered / the level change.
		c.level = level
		c.counter = configs.DetectorInitWaitCnt[level]
		c.react = DelayOneStep
	} else {
		// transition
		if c.counter == 0 {
			// back to initial Level
			c.react = DownTransAction
			c.level = NoCFNoNF
		} else {
			c.react = DelayOneStep
			c.counter--
		}
	}
}

func (c SimpleLearner) Action(cid string) int {
	for {
		rec := c.react
		if rec == NoAction { // not received yet.
			panic("impossible for simple method")
			//time.Sleep(5 * time.Millisecond)
			//continue
		} else {
			c.react = NoAction
			return rec
		}
	}
}
