package detector

import "math/rand"

const (
	MaximumSA      = 1000
	MinimumSA      = 1
	InitialHotness = 200
	CoolingRate    = 0.8
)

type SALearner struct {
	level   Level
	counter int
	react   int

	//
	hotness             float32
	learnedOptimalPoint int
	curTry              int
	curOptimal          float32
}

func (c SALearner) Init(cid string, i int) {
	c.level = NoCFNoNF
	c.counter = 1
	c.react = NoAction
	c.curTry = -1
	c.hotness = InitialHotness
	c.curOptimal = -1
	c.learnedOptimalPoint = (MaximumSA + MinimumSA) / 2
}

func (c SALearner) Send(level Level, cid string, failure bool) {
	if level == 1 {
		c.react = DelayOneStep
		return
	}
	if level != c.level || failure {
		// reset when there is a failure encountered / the level change.
		c.level = level
		if c.hotness > 1 {
			newReward := GetReward()
			if newReward > c.curOptimal && c.curTry != -1 {
				c.curOptimal = newReward
				c.learnedOptimalPoint = c.curTry
			}
			c.curTry = c.learnedOptimalPoint + int(c.hotness*(rand.Float32()*2-1))
			c.counter = c.curTry
			c.hotness *= CoolingRate
		} else {
			// the optimal grid has been selected.
			c.counter = c.learnedOptimalPoint
		}
		c.react = DelayOneStep
	} else {
		// transition
		if c.counter == 0 {
			// back to initial Level
			c.react = DownTransAction
			c.level = DelayOneStep
		} else {
			c.react = DelayOneStep
			c.counter--
		}
	}
}

func (c SALearner) Action(cid string) int {
	for {
		rec := c.react
		if rec == NoAction { // not received yet.
			panic("impossible for grid method")
		} else {
			c.react = NoAction
			return rec
		}
	}
}
