package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(registerChan chan string, jobName string, mapFiles []string, phase jobPhase, i int) {
			defer fmt.Println("Wait group: ", wg)
			defer wg.Done()
			for {
				worker := <-registerChan
				fmt.Printf("llll: %d %d \n", len(mapFiles), i)
				doArgs := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
				res := call(worker, "Worker.DoTask", doArgs, new(struct{}))
				if res {
					// deadlock if not use go routine, sends do not complete
					// until there is a receiver to accept the value
					go func(registerChan chan string){registerChan <- worker}(registerChan)
					break
				} else {
					fmt.Printf("worker: %s in jobname: %s error", worker, jobName)
				}

			}

		}(registerChan, jobName, mapFiles, phase, i)
	}

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
