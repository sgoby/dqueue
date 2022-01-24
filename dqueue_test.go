package dqueue

import (
	"testing"
	"sync"
	"context"
	"time"
	"math/rand"
)



func Test_JobList(t *testing.T){
	listJ := newJobList()
	for j := 0; j < 100;j++ {
		n := rand.Intn(1000)
		listJ.insert(&delayJob{outUnixTime:int64(n)})
	}
	listJ.dump()
	j := listJ.popMin()
	t.Log(j)
	listJ.dump()
}


func Test_put(t *testing.T){
	wg := &sync.WaitGroup{}
	q := NewDQueue(context.Background())
	nowtime := time.Now()
	for i := 0; i < 10000;i++{
		wg.Add(1)
		go func(num int){
			for j := 0; j < 1;j++ {
				q.DelayPut(time.Second*time.Duration(num), "asdfasdf000000")
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	//q.DelayPut(time.Second*1100000,true, "asdfasdf000000")
	t.Log("Use time: ",time.Now().Sub(nowtime))
	//q.dumpAdvanceQueue()
	//
	t.Log(q.Length(),q.DelayLength())
	time.Sleep(time.Second * 10)
	t.Log(q.Length(),q.DelayLength())
	time.Sleep(time.Second * 5)
	t.Log(q.Length(),q.DelayLength())
}