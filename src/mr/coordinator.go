package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkState int

const (
	IDLE WorkState = iota
	IN_PROGRESS
	COMPLETED
)

type Coordinator struct {
	// Your definitions here.
	inputFiles          []string
	mapperState         []WorkState
	reducerState        []WorkState
	mapperResultFiles   [][]string
	reducerNumber       int
	finished            bool
	remainMapperNumber  int
	remainReducerNumber int

	mapFinished sync.Cond
	mutex       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) timeoutMapperState(index int) {
	time.Sleep(10 * time.Second)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.mapperState[index] != COMPLETED {
		fmt.Printf("Reset mapper state %d\n", index)
		c.mapperState[index] = IDLE
	}
}

func (c *Coordinator) timeoutReducerState(index int) {
	time.Sleep(10 * time.Second)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.mapperState[index] != COMPLETED {
		fmt.Printf("Reset reducer state %d\n", index)
		c.mapperState[index] = IDLE
	}
}

func (c *Coordinator) AllocWork(args *AllocWorkerArgs, reply *AllocWorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.finished {
		fmt.Println("finish task")
		reply.Kind = DONE
		return nil
	}

	for index, state := range c.mapperState {
		if state == IDLE {
			fmt.Printf("alloc mapper %d\n", index)
			reply.Index = index
			reply.InputFilePath = c.inputFiles[index]
			reply.ReducerNumber = c.reducerNumber
			reply.Kind = MAPPER
			c.mapperState[index] = IN_PROGRESS
			go c.timeoutMapperState(index)
			return nil
		}
	}

	if c.remainMapperNumber == 0 {
		for index, state := range c.reducerState {
			if state == IDLE {
				fmt.Printf("alloc reducer")
				reply.Index = index
				reply.Kind = REDUCER
				reply.InputFilePath = ""
				c.reducerState[index] = IN_PROGRESS
				go c.timeoutReducerState(index)
				return nil
			}
		}
	}

	reply.Kind = NONE
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	nMap := len(files)
	fmt.Printf("nMapper %d, nReducer %d\n", nMap, nReduce)
	c := Coordinator{
		inputFiles:          files,
		reducerNumber:       nReduce,
		mapperState:         make([]WorkState, nMap),
		reducerState:        make([]WorkState, nReduce),
		mapperResultFiles:   make([][]string, nMap),
		remainMapperNumber:  nMap,
		remainReducerNumber: nReduce,
		finished:            false,
		mutex:               sync.Mutex{},
	}
	c.mapFinished = *sync.NewCond(&c.mutex)

	c.server()
	return &c
}
