package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
GetTaskLoop:
	for {
		task := *GetTask()
		switch task.Type {
		case MapType:
			{
				DoMapTask(&task, mapf)
				ReportFinished(&task)
				break GetTaskLoop
			}
		case ReduceType:
			{
				break GetTaskLoop
			}
		case WaitType:
			{
				//todo
				break GetTaskLoop
			}
		case ExitType:
			{
				break GetTaskLoop
			}

		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	for _, filename := range task.TargetFiles {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("[Error] Cannot open %v!", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			fmt.Printf("[Error] Cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	HashedKVA := make([][]KeyValue, task.ReduceNum)
	for _, kv := range intermediate {
		HashedKVA[ihash(kv.Key)%task.ReduceNum] = append(HashedKVA[ihash(kv.Key)%task.ReduceNum], kv)
	}

	os.Mkdir("./mr-tmp", 0755)
	for i := 0; i < task.ReduceNum; i++ {
		oname := "./mr-tmp/mr-tmp-" + strconv.Itoa(task.UTID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKVA[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

// RPCs

func GetTask() *Task {
	args := GetTaskArgs{}
	reply := Task{}

	ok := call("Coordinator.PullTask", &args, &reply)
	if ok {
		fmt.Println("[Info] rpc Coordinator.PullTask successfully executed")
		fmt.Println("[Info] reply Task:", reply)
	} else {
		fmt.Println("[Error] rpc Coordinator.PullTask failed!")
	}
	return &reply
}

func ReportFinished(task *Task) {
	args := ReportFinishedArgs{TUID: task.UTID}
	reply := ReportFinishedReply{}

	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		fmt.Println("[Info] rpc Coordinator.MarkFinished successfully executed")
		fmt.Println("[Info] reply Task:", reply)
	} else {
		fmt.Println("[Error] rpc Coordinator.MarkFinished failed!")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
