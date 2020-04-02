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

func (mr *MapReduce) DoJobHelper(jobNumber int, jobType JobType, jobChannel chan bool) {
	// Will be called nMap times and nReduce times
	workerAddress := <-mr.registerChannel // Name of worker. BLOCKS UNTIL WORKER IS READY

	var numOtherPhase int
	if jobType == Map {
		numOtherPhase = mr.nReduce
	} else {
		numOtherPhase = mr.nMap
	}

	args := &DoJobArgs{mr.file, jobType, jobNumber, numOtherPhase}
	var reply DoJobReply

	ok := call(workerAddress, "Worker.DoJob", args, &reply) //Don't Need to Check Code, Worker Won't Fail Call

	if ok == true {
		jobChannel <- true                  //AFTER ITS DONE
		mr.registerChannel <- workerAddress // No Failure. Can't Put, unless it's flushed. Ready to Go!
		//STUCK HERE, mr.registerChannel is FULL and no one is here to read it :(
	} else {
		mr.DoJobHelper(jobNumber, jobType, jobChannel) // TRY AGAIN! Maybe with Same Worker, Maybe with Different Worker
	}
}

func synchronizationBarrier(channelSize int, jobchannel chan bool) {
	for i := 0; i < channelSize; i++ {
		<-jobchannel //Need to Get Rid of Contents channelSize times
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Sends to a buffered channel block only when the buffer is full.
	// Receives block when the buffer is empty.

	// Read from mr.registerChannel to get NEW WORKERS

	// Only needs to tell the workers Job Number and File Name

	nMapSynchronizationChannel := make(chan bool, mr.nMap)
	nReduceSynchronizationChannel := make(chan bool, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		go mr.DoJobHelper(i, Map, nMapSynchronizationChannel) // Concurrent Call
	}

	synchronizationBarrier(mr.nMap, nMapSynchronizationChannel)
	// Need to wait until MAP are all done!

	for i := 0; i < mr.nReduce; i++ {
		go mr.DoJobHelper(i, Reduce, nReduceSynchronizationChannel) // Concurrent Call
	}

	synchronizationBarrier(mr.nReduce, nReduceSynchronizationChannel)
	//Can only start merging once Reduce are all done!

	//If Jobs Finish Return
	return mr.KillWorkers()
}
