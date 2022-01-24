package dqueue

import (
	"time"
	"sync"
	"context"
	"log"
)

//
const defaultSize = 10240

type jobDelayQueue interface {
	insert(item *delayJob)
	getMin() *delayJob
	popMin() *delayJob
	len() int
	dump()
}

//
type DQueue struct {
	generalQueue   []interface{}
	generalMu      *sync.Mutex
	ctx            context.Context
	ctxCancel      func()
	mJobDelayQueue jobDelayQueue
}

//
func NewDQueue(ctx context.Context) *DQueue {
	q := &DQueue{
		generalQueue:   make([]interface{}, 0, defaultSize),
		generalMu:      new(sync.Mutex),
		mJobDelayQueue: newJobList(),
	}
	q.ctx, q.ctxCancel = context.WithCancel(ctx)
	go q.runDelayWatch()
	return q
}

//
func (q *DQueue) Put(vals ...interface{}) (n int) {
	n = len(vals)
	if n > 0 {
		q.generalMu.Lock()
		if len(q.generalQueue)+len(vals) > cap(q.generalQueue) {
			newQueue := make([]interface{}, len(q.generalQueue), cap(q.generalQueue)+defaultSize)
			copy(newQueue, q.generalQueue)
			q.generalQueue = newQueue
		}
		q.generalQueue = append(q.generalQueue, vals...)
		q.generalMu.Unlock()
	}
	return n
}

//
func (q *DQueue) DelayPut(delay time.Duration, vals ...interface{}) int {
	if delay <= 0 {
		return q.Put(vals...)
	}
	nowTime := time.Now()
	for _, v := range vals {
		item := &delayJob{
			outUnixTime: nowTime.Add(delay).Unix(),
			val:         v,
		}
		q.mJobDelayQueue.insert(item)
	}
	return len(vals)
}

//
func (q *DQueue) dumpAdvanceQueue() {
	q.mJobDelayQueue.dump()
	log.Println("---------------------")
}

//
func (q *DQueue) Pop() (val interface{}, ok bool) {
	n := len(q.generalQueue)
	if n > 0 {
		val = q.generalQueue[0]
		//
		q.generalMu.Lock()
		q.generalQueue = q.generalQueue[1:]
		q.generalMu.Unlock()
		return val, true
	}
	return nil, false
}

//
func (q *DQueue) runDelayWatch() {
	for {
		select {
		case <-q.ctx.Done():
			break
		default:
			n := q.delayWatch()
			if n <= 0 {
				time.Sleep(time.Second)
			}
		}
	}
}

//
func (q *DQueue) delayWatch() int {
	nowTime := time.Now()
	job := q.mJobDelayQueue.getMin()
	if job == nil || job.outUnixTime > nowTime.Unix() {
		return 0
	}
	job = q.mJobDelayQueue.popMin()
	q.Put(job.val)
	return 1
}

//
func (q *DQueue) Length() int {
	return len(q.generalQueue)
}

//
func (q *DQueue) DelayLength() int {
	return q.mJobDelayQueue.len()
}

//
func (q *DQueue) Close() {
	if q.ctxCancel != nil {
		q.ctxCancel()
	}
}
