package detector

var gridSteps = []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, -1}

type GridLearner struct {
	level   Level
	counter int
	react   int

	//
	learnedOptimalGrid int
	index              int
	curOptimal         float32
}

func (c GridLearner) Init(cid string, i int) {
	c.level = NoCFNoNF
	c.counter = 1
	c.react = NoAction
	c.index = 0
	c.curOptimal = -1
	c.learnedOptimalGrid = 0
}

func (c GridLearner) Send(level Level, cid string, failure bool) {
	if level == 1 {
		c.react = DelayOneStep
		return
	}
	if level != c.level || failure {
		// reset when there is a failure encountered / the level change.
		c.level = level
		if c.index <= len(gridSteps) {
			// in case when the optimal value has not been learnt, try more grids.
			newReward := GetReward()
			if newReward > c.curOptimal && c.index > 0 {
				c.curOptimal = newReward
				c.learnedOptimalGrid = c.index - 1
			}
			c.counter = gridSteps[c.index]
			c.index++
		} else {
			// the optimal grid has been selected.
			c.counter = gridSteps[c.learnedOptimalGrid]
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

func (c GridLearner) Action(cid string) int {
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
