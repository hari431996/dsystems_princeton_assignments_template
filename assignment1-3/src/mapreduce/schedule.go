package mapreduce

import (
	"fmt"
	"sync"
)

type taskData struct{
	fileName string
	TaskNumber int
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	indexQueue := []taskData{}
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce

		for i , val := range mr.files{
			
			indexQueue = append(indexQueue, taskData{val, i})
		}

		

	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)

		for i := 0; i < ntasks ; i++ {
			
			indexQueue = append(indexQueue, taskData{"", i})
		}


	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	
	fmt.Println(phase, indexQueue)
	

	// we need this waitgroup to make sure that only once map is done we move onto reduce phase.
	var wg sync.WaitGroup


	for (len(indexQueue) > 0){
		select {
		case worker := <-mr.registerChannel:
			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			if phase == mapPhase{
				args.File = indexQueue[0].fileName
			}
			
			args.Phase = phase
			args.TaskNumber = indexQueue[0].TaskNumber
			args.NumOtherPhase = nios

			go mr.caller(worker,&wg,  &indexQueue, args)
			
			indexQueue = indexQueue[1:]
			wg.Add(1)
		default:
			
	}

	// only exit after map task is done.

	
}

wg.Wait()

debug("Schedule: %v phase done\n", phase)

}
func (mr *Master) caller(worker string,wg *sync.WaitGroup ,indexQueue *[]taskData,args *DoTaskArgs) {
	// call the worker rpc from here
	ok := call(worker, "Worker.DoTask", &args, new(struct{}))
	wg.Done()
	

	if ok == false{
		fmt.Println("worker down - reassign the work", worker)
		*indexQueue = append(*indexQueue, taskData{args.File, args.TaskNumber})
		return 
	}

	// once the work is done send to schedule that worker is available. 
	
	mr.registerChannel <- worker
	

}
