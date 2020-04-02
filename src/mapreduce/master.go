package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	// Read from mr.registerChannel to get NEW WORKERS

	// Only needs to tell the workers Job Number and File Name
	for i := 0; i < mr.nMap; i++ {
		workerAddress := <-mr.registerChannel // Name of worker

		args := &DoJobArgs{mr.file, "Map", i, mr.nReduce}
		var reply DoJobReply
		go call(workerAddress, "Worker.DoJob", args, reply)
		_ = reply

		fmt.Printf("REACHED")
	}

	for i := 0; i < mr.nReduce; i++ {
		workerAddress := <-mr.registerChannel // Name of worker
		args := &DoJobArgs{mr.file, "Reduce", i, mr.nMap}
		var reply DoJobReply
		go call(workerAddress, "Worker.DoJob", args, reply)
		_ = reply

	}

	//If Jobs Finish Return
	return mr.KillWorkers()
}
