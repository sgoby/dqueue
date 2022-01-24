package dqueue

import (
	"sync"
	"log"
)

type delayJob struct {
	outUnixTime int64
	val         interface{}
	next, prev  *delayJob
	nextStep10  *delayJob
	nextStep50  *delayJob
	nextStep100 *delayJob
}

type jobList struct {
	root         *delayJob
	size         int
	mu           *sync.Mutex
	stepStart10  *delayJob
	stepStart50  *delayJob
	stepStart100 *delayJob
}

func newJobList() *jobList {
	return &jobList{
		mu: new(sync.Mutex),
	}
}

//
func (l *jobList) insert(j *delayJob) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.size < 1 {
		l.root = j
	} else {
		prevJob := l.maxLtJob(j)
		if prevJob == nil {
			l.root.prev = j
			j.next = l.root
			l.root = j
		} else {
			j.next = prevJob.next
			j.prev = prevJob

			if prevJob.next != nil {
				prevJob.next.prev = j
			}
			prevJob.next = j
		}
	}
	//
	if l.stepStart100 == nil || l.root.outUnixTime > l.stepStart100.outUnixTime{
		l.stepStart100 = l.root
	}
	if l.stepStart50 == nil || l.root.outUnixTime > l.stepStart50.outUnixTime{
		l.stepStart50 = l.root
	}
	if l.stepStart10 == nil || l.root.outUnixTime > l.stepStart10.outUnixTime{
		l.stepStart10 = l.root
	}

	//
	l.size += 1
	if j.outUnixTime-l.stepStart100.outUnixTime >= 200 {
		l.stepStart100.nextStep100 = j
		l.stepStart100 = j
	} else if j.outUnixTime-l.stepStart50.outUnixTime >= 50 {
		l.stepStart50.nextStep50 = j
		l.stepStart50 = j
	} else if j.outUnixTime-l.stepStart10.outUnixTime >= 10 {
		l.stepStart10.nextStep10 = j
		l.stepStart10 = j
	}

}

//found the max job that less than current job
func (l *jobList) maxLtJob(j *delayJob) *delayJob {
	if l.root == nil {
		return nil
	}
	job := l.root
	count := 0
	for {
		if job.outUnixTime >= j.outUnixTime {
			return job.prev
		}
		if job.next == nil {
			break
		}
		if job.nextStep100 != nil && job.nextStep100.outUnixTime < j.outUnixTime{
			job = job.nextStep100
		}else if job.nextStep50 != nil && job.nextStep50.outUnixTime < j.outUnixTime{
			job = job.nextStep50
		}else if job.nextStep10 != nil && job.nextStep10.outUnixTime < j.outUnixTime{
			job = job.nextStep10
		}else{
			job = job.next
		}
		count += 1
	}
	if count < 1 {
		if job.outUnixTime <= j.outUnixTime {
			return job
		}
		return nil
	}
	return job
}

//
func (l *jobList) getMin() *delayJob {
	return l.root
}

//
func (l *jobList) popMin() *delayJob {
	l.mu.Lock()
	defer l.mu.Unlock()

	j := l.root
	l.root = l.root.next
	l.root.prev = nil
	l.size -= 1
	return j
}

func (l *jobList) len() int{
	return l.size
}

//
func (l *jobList) dump() {
	if l.root == nil {
		return
	}
	job := l.root
	for {
		log.Println(job.outUnixTime)
		if job.next == nil {
			break
		}
		job = job.next
	}
	log.Println("---------------------")
}
