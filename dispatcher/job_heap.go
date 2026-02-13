package dispatcher

import "time"

type scheduledJob struct {
	job     Job
	attempt int
	runAt   time.Time
}

type scheduledJobHeap []scheduledJob

func (h scheduledJobHeap) Len() int {
	return len(h)
}

func (h scheduledJobHeap) Less(i, j int) bool {
	return h[i].runAt.Before(h[j].runAt)
}

func (h scheduledJobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *scheduledJobHeap) Push(value any) {
	job, ok := value.(scheduledJob)
	if !ok {
		return
	}

	*h = append(*h, job)
}

func (h *scheduledJobHeap) Pop() any {
	old := *h
	oldLength := len(old)

	if oldLength == 0 {
		return scheduledJob{}
	}

	last := old[oldLength-1]
	*h = old[:oldLength-1]

	return last
}
