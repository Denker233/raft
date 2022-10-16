package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int 

const (
	Map    TaskType=1
	Reduce TaskType=2
	Done   TaskType=3
)

// Add your RPC definitions here.
type TaskArgs struct {
	X int
}
type FinishedTaskArgs struct{
	Index int
	TaskType int
}

type TaskReply struct {
	TaskType int
	MapFile  string
	AllFinished bool
	nReduce int
	nMap    int
	Index int

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}