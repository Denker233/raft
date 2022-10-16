package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	args:=TaskArgs{}
	reply=TaskReply{}
	call("Coordinator.TaskHandler", &args, &reply)

	switch reply.TaskType{
	case Map:
		doMap(reply.MapFile, reply.Index, reply.nReduce, mapf)
	case Reduce:
		doReduce(reply.Index, reply.nMap ,reducef)
	case Done:
		os.Exit(0)
	default:
		fmt.Errorf("Wierd TaskType %s",reply.TaskType)
	}
	args:=FinishedTaskArgsq{
		Index: reply.Index,
		TaskType: reply.TaskType,
	}

	call("Coordinator.FinishedTaskHandler", &args, &reply)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()



}

func doMap(fname string, Index int, nReduce int, mapf func(string, string) []KeyValue){
	file, err := os.Open(fname)
	if err !=nil{
		log.Fatalf("not opening %v", fname)

	}
	content. err:= ioutil.ReadAll(file)
	if err !=nil{
		log.Fatalf("not reading %v", fname)
	}
	file.Close

	kva:=mapf(fname,string(content))
	reduceKva := make([][]KeyValue, nReduce)

	for _, kv:= range kva {
		reduceIndex:= ihash(kv.Key) % nReduce
		reduceKva[reduceIndex] = append(reduceKva[reduceIndex],kv)
	}

	for i, kvs := range reduceKva {
		sort.Sort(ByKey(kvs))


		//store the intermediate in the disk
		filename := fmt.Sprintf("mr-%d-%d", Index, i)
		file, err := os.Create(filename + ".tmp")
		if err != nil {
			log.Fatalf("cannot write %v", filename + ".tmp")
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot jsonencode %v", filename + ".tmp")
			}
		}
		file.Close()
		os.Rename(filename + ".tmp", filename)
		
	} 
	



}

func doReduce(Index int, nMap int, reducef func(string, []string) string ){
	kvs := make([]KeyValue, 0)
	for i:=0; i < nMap;i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, Index)
		fp, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(fp)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	fp.close()

	sort.Sort(ByKey(kvs)) 

	ofileName := fmt.Sprintf("mr-out-%d", Index)
	ofile, err  := os.Create(ofileName + ".tmp")
	if err != nil {
		log.Fatalf("cannot open %v", ofileName + ".tmp")
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {//collect all same key values and since keys are sorted they are grouped together
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofileName + ".tmp", ofileName)


	
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
