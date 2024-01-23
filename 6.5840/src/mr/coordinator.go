package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	CurrentStage      Stage
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task
	TaskInfoMap       map[int]*TaskInfo
	ReduceNum         int
	UTID              int
	mu                *sync.Mutex
}

func (c *Coordinator) MakeMapTasks(files []string) {
	for _, fileName := range files {
		utid := c.getNewUTID()
		task := Task{
			UTID:        utid,
			Type:        MapType,
			TargetFiles: []string{fileName},
			ReduceNum:   c.ReduceNum,
		}
		c.MapTaskChannel <- &task
		taskInfo := TaskInfo{
			Status:   Waiting,
			TaskAddr: &task,
		}
		c.TaskInfoMap[utid] = &taskInfo
	}
}

func (c *Coordinator) getNewUTID() int {
	c.UTID += 1
	return c.UTID
}

// Your code here -- RPC handlers for the worker to call.

// workers ask for new job
func (c *Coordinator) PullTask(args *GetTaskArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.CurrentStage {
	case MapStage:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				taskInfo, ok := c.TaskInfoMap[reply.UTID]
				if !ok || taskInfo == nil || taskInfo.Status != Waiting {
					fmt.Println("[Error]", reply, "does not exist in TaskInfoMap or Assigned to worker repeatedly!")
					panic(1)
				}
				taskInfo.Status = Processing
			} else {
				reply.Type = WaitType
				if c.checkStageOver() {
					c.NextStage()
				}
			}
		}
	// case ReduceStage:
	// 	{
	// 		*reply = *<-c.ReduceTaskChannel
	// 	}
	default:
		{
			reply.Type = ExitType
		}
	}
	return nil
}

func (c *Coordinator) MarkFinished(args *ReportFinishedArgs, reply *ReportFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.TaskInfoMap[args.TUID].Status {
	case Processing:
		{
			fmt.Println("[Info]task ", *c.TaskInfoMap[args.TUID].TaskAddr, " is done")
		}
	case Waiting:
		{
			fmt.Println("[Error]task ", *c.TaskInfoMap[args.TUID].TaskAddr, " is done, without processing!!!")
		}
	case Done:
		{
			fmt.Println("[Warnning]task ", *c.TaskInfoMap[args.TUID].TaskAddr, " is done again!")
		}
	default:
		panic(1)
	}
	c.TaskInfoMap[args.TUID].Status = Done
	return nil
}

func (c *Coordinator) checkStageOver() bool {
	for _, taskInfo := range c.TaskInfoMap {
		if taskInfo.Status != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) NextStage() {
	fmt.Println("NEXT STAGE!")
	switch c.CurrentStage {
	case MapStage:
		{
			c.CurrentStage = ExitStage
		}
	case ReduceStage:
		{
			c.CurrentStage = ExitStage
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		CurrentStage:      MapStage,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskInfoMap:       make(map[int]*TaskInfo, len(files)+nReduce),
		ReduceNum:         nReduce,
		UTID:              0,
		mu:                new(sync.Mutex),
	}
	c.MakeMapTasks(files)
	fmt.Println("Map Tasks initializing complete!")
	fmt.Println(c.TaskInfoMap)
	c.server()
	return &c
}
