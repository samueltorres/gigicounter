package gcounter

type GCounterService struct {
	counters map[string]GCounter
}

type GCounter struct {
	vector map[string]int64
}
