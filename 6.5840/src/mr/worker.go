package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			}
		case ReduceType:
			{
				DoReduceTask(&task, reducef)
				ReportFinished(&task)
			}
		case WaitType:
			{
				//todo
				time.Sleep(time.Second)
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

func getReduceIndex(fileName string) (string, error) {
	re := regexp.MustCompile(`^.*-(\d+)$`)
	matches := re.FindStringSubmatch(fileName)
	if len(matches) != 2 {
		return "", errors.New("[Error] Invalid task file:" + fileName)
	}
	return matches[1], nil
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	if len(task.TargetFiles) == 0 {
		fmt.Println("[Error] Empty task!")
		return
	}
	reduceIndexStr, err := getReduceIndex(task.TargetFiles[0])
	if err != nil {
		fmt.Print(err.Error())
		return
	}

	kva := *shuffleKVArray(&task.TargetFiles)

	oname := "mr-out-" + reduceIndexStr
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

func shuffleKVArray(files *[]string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filename := range *files {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("[Error] Cannot open %v!", filename)
			panic(1)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		kv := KeyValue{}
	ReadIntermediateFileLoop:
		for {
			err := decoder.Decode((&kv))
			if err != nil {
				if err == io.EOF {
					break ReadIntermediateFileLoop
				}
				fmt.Println("[Error] Cannot decoding JSON:", err)
				break ReadIntermediateFileLoop
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	return &kva
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

	for i := 0; i < task.ReduceNum; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.UTID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		sort.Sort(ByKey(HashedKVA[i]))
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
		//fmt.Println("[Info] rpc Coordinator.PullTask successfully executed")
		//fmt.Println("[Info] reply Task:", reply)
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
		//fmt.Println("[Info] rpc Coordinator.MarkFinished successfully executed")
		//fmt.Println("[Info] reply Task:", reply)
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
