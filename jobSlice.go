package dqueue

import (
	"sync"
)

//
type sliceQueue struct {
	advanceQueue []*delayJob
	advanceMu    *sync.Mutex
}

func newSliceQueue() *sliceQueue{
	return &sliceQueue{
		advanceQueue: make([]*delayJob, 0, defaultSize),
		advanceMu:    new(sync.Mutex),
	}
}

//
func (q *sliceQueue) insert(item *delayJob) {
	q.advanceMu.Lock()
	if len(q.advanceQueue)+1 > cap(q.advanceQueue) {
		newQueue := make([]*delayJob, len(q.advanceQueue), cap(q.advanceQueue)+defaultSize)
		copy(newQueue, q.advanceQueue)
		q.advanceQueue = newQueue
	}
	q.advanceQueue = append(q.advanceQueue, item)
	//
	l := len(q.advanceQueue) - 1
	lastEnd := l
	if l > 0 {
		midIdx := l / 2
		for {
			if q.advanceQueue[midIdx].outUnixTime > item.outUnixTime {
				//left
				if midIdx <= 0 {
					copy(q.advanceQueue[1:], q.advanceQueue[0:])
					q.advanceQueue[0] = item
					break
				} else if q.advanceQueue[midIdx-1].outUnixTime <= item.outUnixTime {
					copy(q.advanceQueue[midIdx+1:], q.advanceQueue[midIdx:])
					q.advanceQueue[midIdx] = item
					break
				}

				lastEnd = midIdx
				midIdx = midIdx / 2
			} else if q.advanceQueue[midIdx].outUnixTime < item.outUnixTime {
				//right
				if midIdx+1 >= lastEnd {
					copy(q.advanceQueue[lastEnd+1:], q.advanceQueue[lastEnd:])
					q.advanceQueue[lastEnd] = item
					break
				} else if q.advanceQueue[midIdx+1].outUnixTime >= item.outUnixTime {
					copy(q.advanceQueue[midIdx+2:], q.advanceQueue[midIdx+1:])
					q.advanceQueue[midIdx+1] = item
					break
				}
				midIdx = midIdx + (lastEnd-midIdx)/2
			} else {
				if midIdx+1 <= l {
					copy(q.advanceQueue[midIdx+1:], q.advanceQueue[midIdx:])
					q.advanceQueue[midIdx] = item
				}
				break
			}
		}
	}
	q.advanceMu.Unlock()
}

//
func (q *sliceQueue) getMin() *delayJob {
	if len(q.advanceQueue) >0 {
		return q.advanceQueue[0]
	}
	return nil
}

//
func (q *sliceQueue) popMin() *delayJob {
	q.advanceMu.Lock()
	defer q.advanceMu.Unlock()

	if len(q.advanceQueue) >0 {
		item :=  q.advanceQueue[0]
		q.advanceQueue = q.advanceQueue[1:]
		return item
	}
	return nil
}

func (q *sliceQueue) len() int{
	return len(q.advanceQueue)
}

func (q *sliceQueue) dump(){

}