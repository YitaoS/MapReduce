package mr

import "time"

type Task struct {
	UTID        int
	Type        TaskType
	TargetFiles []string
	ReduceNum   int
}

type TaskInfo struct {
	Status    TaskStatus
	TaskAddr  *Task
	StartTime time.Time
}

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
	WaitType
	ExitType
)

type TaskStatus int

const (
	Processing TaskStatus = iota
	Waiting
	Done
)

type Stage int

const (
	MapStage Stage = iota
	ReduceStage
	ExitStage
)
