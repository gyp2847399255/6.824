package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		alloc := CallAllocWork()
		if alloc.Kind == DONE {
			return
		}

		if alloc.Kind == NONE {
			time.Sleep(time.Second)
			continue
		}

		if alloc.Kind == MAPPER {

		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func mapper(inputFilePath string, index int, reduceNumber int,
	mapFunction func(string, string) []KeyValue) []string {
	file, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("can't open %s\n", inputFilePath)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read %s\n", inputFilePath)
	}
	keyValues := mapFunction(inputFilePath, string(content))

	var encoders []*json.Encoder
	var outputFiles []*os.File
	for i := 0; i < reduceNumber; i++ {
		temp, err := ioutil.TempFile(".", "mapper-temp-*")
		if err != nil {
			log.Fatalf("can't create temp file %s", err.Error())
		}
		outputFiles = append(outputFiles, temp)
		encoders = append(encoders, json.NewEncoder(temp))
	}

	for _, pair := range keyValues {
		hash := ihash(pair.Key)
		enc := encoders[hash % reduceNumber]
		enc.Encode(&pair)
	}
	var output []string
	for p, f := range outputFiles {
		name := f.Name()
		f.Close()
		outFile := fmt.Sprintf("mr-%d-%d", index, p)
		err = os.Rename(name, outFile)
		if err != nil {
			log.Fatalf("failed to rename %s", name)
		}
		output = append(output, outFile)
	}

	return output
}

func CallAllocWork() *AllocWorkerReply {
	args := new(AllocWorkerArgs)
	reply := new(AllocWorkerReply)
	call("Coordinator.AllocWork", args, reply)
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
