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

func (c *Coordinator) FinishWork(args *FinishWorkArgs, reply *FinishWorkReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.Kind {
	case DONE:
		return nil
	case MAPPER:
		fmt.Printf("finish mapper %d\n", args.Index)
		c.mapperState[args.Index] = COMPLETED
		c.mapperResultFiles[args.Index] = args.ResultFilePaths
		c.remainMapperNumber -= 1
		if c.remainMapperNumber == 0 {
			c.mapFinished.Broadcast()
		}
		return nil
	case REDUCER:
		fmt.Printf("finish reducer %d\n", args.Index)
		c.reducerState[args.Index] = COMPLETED
		c.remainReducerNumber -= 1
		if c.remainReducerNumber == 0 {
			c.finished = true
		}
		return nil
	}
	return nil
}

func (c *Coordinator) AskMapResult(args *AskMapResultArgs, reply *AskMapResultReply) error {
	fmt.Printf("reducer %d ask map result\n", args.ReducerIndex)
	c.mutex.Lock()
	if c.remainMapperNumber != 0 {
		fmt.Println("wait for mapper finish")
		c.mapFinished.Wait()
		fmt.Println("mapper condition released")
	}
	c.mutex.Unlock()

	ret := make([]string, len(c.inputFiles))
	for i, files := range c.mapperResultFiles {
		ret[i] = files[args.ReducerIndex]
	}

	reply.FilePaths = ret
	fmt.Printf("reply for reducer %d\n", args.ReducerIndex)
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

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, state := range c.mapperState {
		if state != COMPLETED {
			return false
		}
	}

	for _, state := range c.reducerState {
		if state != COMPLETED {
			return false
		}
	}

	c.finished = true
	return true
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
