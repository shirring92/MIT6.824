package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Master struct {
	// Your definitions here.
	workerId int
	killmap map[string]bool
	nMap int
	nReduce int
	mapCnt int
	reduceCnt int
	allMapDone bool
	allReduceDone bool
	mapTaskStatus map[int]TaskStatus
	reduceTaskStatus map[int]TaskStatus
	mapQ []Task
	reduceQ []Task
	mu sync.Mutex
}

type TaskStatus struct {
	workerId string
	status bool
}

type Task struct {
	id int
	name string
	files []string
}

// Your code here -- RPC handlers for the worker to call.

// register worker with a unique workerid
// workerid is used for master to know which task assigned to which worker
func (m *Master) AddWorker(args *ArgsJoin, reply *ReplyJoin) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.WorkerId = fmt.Sprintf("worker%d", m.workerId)
	m.killmap[reply.WorkerId] = false
	m.workerId++

	return nil
}

func (m *Master) AssignTask(args *ArgsForTask, reply *ReplyWithFile) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.NReduce = m.nReduce

	if m.killmap[args.WorkerId] {
		reply.Id = -1
		reply.Taskname = "exit"
		return nil
	}

	t := Task{}
	t.id = -1
	t.name = "wait"

	if !m.allMapDone {
		if len(m.mapQ) > 0 {
			t = m.mapQ[0]
			m.mapQ = m.mapQ[1:]
			m.mapTaskStatus[t.id] = TaskStatus{args.WorkerId, false}
			// fmt.Println("master assign map task", t.id, t.files[0], "to", args.WorkerId)
		}
	} else if !m.allReduceDone {
		if len(m.reduceQ) > 0 {
			t = m.reduceQ[0]
			m.reduceQ = m.reduceQ[1:]
			m.reduceTaskStatus[t.id] = TaskStatus{args.WorkerId, false}

			// fmt.Println("master assign reduce task", t.id, "to", args.WorkerId)
		}
	} else {
		t.name = "exit"
	}

	reply.Id = t.id
	reply.Taskname = t.name
	reply.Files = t.files

	go m.timer(args.WorkerId, t)

	return nil
}

// start a new goroutine for timer
// if task not finished after 12 seconds, recycle task
// 10 seconds is too short for worker to finish a task on local machine
func (m *Master) timer(workerid string, t Task) {
	time.Sleep(12 * time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()

	if t.name == "map" {
		if m.mapTaskStatus[t.id].workerId == workerid && !m.mapTaskStatus[t.id].status {
			// fmt.Println("map task", t.id, "not finished in 10s")
			m.killmap[workerid] = true
			m.mapTaskStatus[t.id] = TaskStatus{"", false}
			m.mapQ = append(m.mapQ, t)
		}
	} else if t.name == "reduce" {
		if m.reduceTaskStatus[t.id].workerId == workerid && !m.reduceTaskStatus[t.id].status {
			// fmt.Println("reduce task", t.id, "not finished in 10s")
			m.killmap[workerid] = true
			m.reduceTaskStatus[t.id] = TaskStatus{"", false}
			m.reduceQ = append(m.reduceQ, t)
		}
	}
}

// worker let master know it finish the assigned task
// if this task is already assigned to other worker, master ignore this worker
func (m *Master) FinishTask(arg *FinishArgs, reply *FinishReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if arg.Taskname == "map" {
		if m.mapTaskStatus[arg.Id].workerId == arg.WorkerId {
			m.mapTaskStatus[arg.Id] = TaskStatus{arg.WorkerId, true}
			m.mapCnt++
			if m.mapCnt == m.nMap {
				m.allMapDone = true
			}
		}
	} else if arg.Taskname == "reduce" {
		if m.reduceTaskStatus[arg.Id].workerId == arg.WorkerId {
			m.reduceTaskStatus[arg.Id] = TaskStatus{arg.WorkerId, true}
			m.reduceCnt++
			if m.reduceCnt == m.nReduce {
				m.allReduceDone = true
			}
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.allReduceDone {
		ret = true
	}

	return ret
}

// initiate master with map task and reduce task
func (m *Master) init(files []string, nReduce int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workerId = 0
	m.killmap = make(map[string]bool)
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapCnt = 0
	m.reduceCnt = 0
	m.allMapDone = false
	m.allReduceDone = false

	m.mapTaskStatus = make(map[int]TaskStatus)
	for i := 0; i < m.nMap; i++ {
		t := Task{}
		t.id = i
		t.name = "map"
		t.files = append(t.files, files[i])

		m.mapQ = append(m.mapQ, t)
		m.mapTaskStatus[i] = TaskStatus{"", false}
	}

	m.reduceTaskStatus = make(map[int]TaskStatus)
	for i := 0; i < nReduce; i++ {
		t := Task{}
		t.id = i
		t.name = "reduce"

		for j := 0; j < m.nMap; j++ {
			t.files = append(t.files, fmt.Sprintf("mr-%d-%d", j, i))
		}

		m.reduceQ = append(m.reduceQ, t)
		m.mapTaskStatus[i] = TaskStatus{"", false}
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init(files, nReduce)

	m.server()
	return &m
}
