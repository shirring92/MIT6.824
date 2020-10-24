package mr

import (
	"fmt"
	"os"
	"io/ioutil"
	"log"
	"net/rpc"
	"hash/fnv"
	"sort"
	"encoding/json"
	"time"
)


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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workerid string
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := ArgsJoin{}
	reply := ReplyJoin{}
	call("Master.AddWorker", &args, &reply)

	workerid = reply.WorkerId

	// Your worker implementation here.
	for {
		// fmt.Println("worker ask for next task")
		ret := CallForTask(mapf, reducef)
		if !ret {
			break
		}
		time.Sleep(time.Second)
	}
	// 

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func CallForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	args := ArgsForTask{}
	reply := ReplyWithFile{}

	// ask master for task. get file name
	args.WorkerId = workerid
	call("Master.AssignTask", &args, &reply)

	if reply.Taskname == "exit" {
		return false
	}

	if reply.Taskname == "wait" {
		return true
	}
	
	if reply.Taskname == "map" {
		ProcessMapTask(mapf, reply.Id, reply.NReduce, reply.Files)
	} else if reply.Taskname == "reduce" {
		ProcessReduceTask(reducef, reply.Id, reply.Files)
	}

	fargs := FinishArgs{}
	freply := FinishReply{}
	fargs.WorkerId = workerid
	fargs.Id = reply.Id
	fargs.Taskname = reply.Taskname
	call("Master.FinishTask", &fargs, &freply)

	return true
}

func ProcessMapTask(mapf func(string, string) []KeyValue, taskid int, nReduce int, files []string) {
	file, err := os.Open(files[0])
	if err != nil {
		log.Fatalf("cannot open %v", files[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", files[0])
	}
	file.Close()
		

	kva := mapf(files[0], string(content))

	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", taskid, i)
		ofile, _ := os.Create(oname)
		ofile.Close()
	}

	for i := 0; i < len(kva); i++ {
		y := ihash(kva[i].Key) % nReduce
		oname := fmt.Sprintf("mr-%d-%d", taskid, y)
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

		enc := json.NewEncoder(ofile)
		err := enc.Encode(&kva[i])
		if err != nil {
			// fmt.Println("cannot encode", err)
		}

		ofile.Close()
	}
}

func ProcessReduceTask(reducef func(string, []string) string, taskid int, files []string) {
	kva := []KeyValue{}
	for k := 0; k < len(files); k++ {
		file, err := os.Open(files[k])
		if err != nil {
			log.Fatalf("cannot open %v", files[k])
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", taskid)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
		
	ofile.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// fmt.Println("client call")
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
