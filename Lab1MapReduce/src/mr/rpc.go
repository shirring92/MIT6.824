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

// Add your RPC definitions here.
type ArgsJoin struct {

}

type ReplyJoin struct {
	WorkerId string
}

type ArgsForTask struct {
	WorkerId string // id of the specific worker
}

type ReplyWithFile struct {
	Taskname string
	Files []string
	Id int
	NReduce int
}

type FinishArgs struct {
	WorkerId string 
	Id int
	Taskname string
}

type FinishReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
