package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	mu sync.Mutex

	mapFiles []string
	nMapTasks int
	nReduceTasks int
	AllMap []*MapTask
	AllReduce []*ReduceTask
	mapTaskFinished []bool
	reduceTaskFinished []bool
	mapAllFinished bool
	reduceAllFinished bool




}

type MapTask struct{
	Index        int      
	Assigned     bool      
	AssignedTime time.Time 
	IsFinished   bool     

	InputFile    string 
	ReducerCount int 
	timeoutTimer *time.Timer

}
type ReduceTask struct {
	Index        int       
	Assigned     bool      
	AssignedTime time.Time 
	IsFinished   bool      

	MapperCount int 

	timeoutTimer *time.Timer 
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	reply.nReduce=c.nReduceTasks
	reply.nMap=c.nMapTasks

	for !c.mapAllFinished{
		c.mapAllFinished = true 
		for _,mapTask :=range c.AllMap{
			if mapTask.Assigned {
				continue
			}
			if mapTask.AssignedTime.IsZero() ||
			time.Since(mapTask.AssignedTime).Seconds()>10{
				mapTask.Assigned = true
				reply.TaskType = Map
				reply.MapFile = mapTask.InputFile
				reply.Index =mapTask.Index
				mapTask.AssignedTime = time.Now()
			}else{
				c.mapAllFinished = false
			}
		}
		if !c.mapAllFinished {
			c.cond.wait()
		}else{
			break
		}
	}

	for !c.reduceAllFinished{
		c.reduceAllFinished= true
		for _, reduceTask := range c.AllReduce{
			if reduceTask.Assigned{
				continue
			}
			if reduceTask.AssignedTime.IsZero()||time.Since(reduceTask.AssignedTime).Seconds()>10{
				reduceTask.Assigned = true
				reply.TaskType = Reduce
				reply.Index = reduceTask.Index
				reduceTask.AssignedTime = time.Now()
				
			}else{
				reduceAllFinished =false
			}

			
		}
		if !reduceAllFinished {
			c.cond.wait()
		}else{
			break
		}
	}
	Taskreply.AllFinished := true
	reply.TaskType=Done
	return




}

func (c *Coordinator) FinishedTaskHandler(args *FinishedTaskArgs, reply *TaskReply) error {
	c.mu.lock()
	defer c.mu.Unlock()

	switch args.TaskType{
	case Map :
		c.mapTasksFinished[args.Index]=true
	case Reduce:
		c.reduceTaskFinished[args.Index]=true
	default:
		log.Fatalf("not finished %s",args.TaskType)

	c.cond.Broadcast()//wake up the line82 && 109
	}

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret:= mapAllFinished && reduceAllFinished

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduceTasks=nReduce
	c.nMapTasks=len(files)
	mapAllFinished := false
	reduceAllFinished := false
	
	c.AllMap=make([]*MapTask, len(files))
	for i , file:= range file{
		mapTask:=MapTask{
			Index:        i,
			Assigned:     false,
			AssignedTime: 0,
			IsFinished:   false,
			InputFile:    file,
			ReduceCount: nReduce,

		}
		c.AllMap=append(c.AllMap,mapTask)
	}

    
	c.AllReduce=make([]* ReduceTask, nReduceTasks)
	for i:=0; i<nReduceTasks;i++{
		reduceTask:=ReduceTask{
			Index:        i,
			Assigned:     false,
			AssignedTime: 0,
			IsFinished:   false,
			MapCount:  len(files),
		}
	}
	c.AllReduce=append(c.AllReduce,reduceTask)

	c.server()
	return &c
}
