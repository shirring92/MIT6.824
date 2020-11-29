package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "math"
import "math/rand"
import "time"
// import "fmt"

// import "bytes"
// import "../labgob"

const (
	Leader = 1
	Follower = 2
	Candidate = 3
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persisten state on all servers
	currentTerm int
	votedFor int
	log []logEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	// 
	state int
	count int
	heartbeatChan chan int
	electionLoseChan chan int

	applyCh chan ApplyMsg
}

// var heartbeatChan chan int

type logEntry struct {
	Command interface{}
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(rf.me, "term", rf.currentTerm, "receive request vote msg from", args.CandidateId, "term", args.Term, "but voted for", rf.votedFor)

	uptodate := false
	lastindex := len(rf.log) - 1
	lastterm := rf.log[lastindex].Term
	if (args.LastLogTerm > lastterm) || (args.LastLogTerm == lastterm && args.LastLogIndex >= lastindex) {
		uptodate = true
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || rf.state != Follower {
		DPrintf("%d on state %d refuse vote to %d", rf.me, rf.state, args.CandidateId)
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.Term > rf.currentTerm) && uptodate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("%d give vote to %d", rf.me, args.CandidateId)
		go rf.receiveHeartbeat(args.CandidateId)
	} else {
		DPrintf("%d refuse vote to %d, state %d, term %d, votedfor %d", rf.me, args.CandidateId, rf.state, rf.currentTerm, rf.votedFor)
		DPrintf("lastindex %d, lastterm %d, requestor %d lastindex %d, lastterm %d", lastindex, lastterm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []logEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
	} else {
		go rf.receiveHeartbeat(args.LeaderId)
		if rf.state == Candidate {
			go rf.breakElection()
		}

		reply.Success = true

		var j = args.PrevLogIndex + 1
		// DPrintf("%d start append log entry at %d", rf.me, j)
		for i := 0; i < len(args.Entries); i++ {
			if j < len(rf.log) {
				rf.log[j] = args.Entries[i]
			} else {
				rf.log = append(rf.log, args.Entries[i])
			}
			j++
		}
		if len(args.Entries) == 0 {
			DPrintf("%d finish heartbeat", rf.me)
		} else {
			DPrintf("%d finish append log entry %d", rf.me, j - 1)
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(j - 1)))
			DPrintf("%d commit index update to %d, log len is %d", rf.me, rf.commitIndex, len(rf.log))
		}
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.state = Follower
		}
		if rf.votedFor != -1 {
			rf.votedFor = -1
		}
	}
}

//
// example code to send a AppendEntries RPC as heartbeats to a server.
//
// func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs) bool {
// 	reply := AppendEntriesReply{}
// 	DPrintf("%d send heartbeat to %d", rf.me, server)

// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)

// 	if ok {
// 		rf.mu.Lock()
// 		defer rf.mu.Unlock()

// 		DPrintf("%d got heartbeat reply from %d", rf.me, server)
// 		if reply.Term > rf.currentTerm {
// 		// fmt.Println(rf.me, "got higher term from", server, "in heartbeat reply")
// 			rf.state = Follower
// 		}
// 	} else {
// 		DPrintf("%d fail to get heartbeat reply from %d", rf.me, server)
// 	}
// 	return ok
// }

//
// example code to send a AppendEntries RPC as heartbeats to a server.
//
func (rf *Raft) sendAppendEntries(server int, term int, len int) bool {
	// for {
		rf.mu.Lock()

		previdx := rf.nextIndex[server] - 1
		args := AppendEntriesArgs{
			Term: term,
			LeaderId: rf.me,
			PrevLogIndex: previdx,
			PrevLogTerm: rf.log[previdx].Term,
			LeaderCommit: rf.commitIndex,
		}
		for i := rf.nextIndex[server]; i < len; i++ {
			args.Entries = append(args.Entries, rf.log[i])
		}
		DPrintf("%d send AE RPC to %d", rf.me, server)
		DPrintf("term %d, prevLogIndex %d, PrevLogTerm %d, LeaderCommit %d", args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			// DPrintf("%d fail to connect with %d on AE RPC on term %d, state is %v", rf.me, server, args.Term, rf.state)
			// time.Sleep(30 * time.Millisecond)
			return false
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%d reply with %v on term %d", server, reply.Success, reply.Term)
		if reply.Success {
			// rf.cntAEReply++;
			rf.nextIndex[server] = len
			rf.matchIndex[server] = len - 1
			DPrintf("%d match %d index to %d", rf.me, server, rf.matchIndex[server])
			// rf.mu.Unlock()
			return true
		} else if reply.Term > rf.currentTerm {
			DPrintf("%d got reply from %d with term %d", rf.me, server, reply.Term)
			rf.currentTerm = reply.Term
			rf.state = Follower
			// rf.mu.Unlock()
			return false
		} else if rf.nextIndex[server] == 0 {
			// rf.mu.Unlock()
			return false
		} else {
			rf.nextIndex[server]--
			DPrintf("%d reduce %d nextIndex to %d", rf.me, server, rf.nextIndex[server])
			return false
		}
		// rf.mu.Unlock()

	// 	time.Sleep(100 * time.Millisecond)
	// }

	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == Leader
	if isLeader {
		DPrintf("%d start send command %v on term %d", rf.me, command, rf.currentTerm)
		index = len(rf.log)
		term = rf.currentTerm
		entry := logEntry{
			Command: command,
			Term: rf.currentTerm,
		}
		rf.log = append(rf.log, entry)
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}

	return index, term, isLeader
}

func (rf *Raft) sendApplyMsg(index int) {
	// DPrintf("%d send apply msg %v to applyCh", rf.me, index)
	
	rf.mu.Lock()
	msg := ApplyMsg{
		CommandValid: true,
		Command: rf.log[index].Command,
		CommandIndex: index,
	}
	DPrintf("%d send apply msg %v of %d to applyCh", rf.me, index, msg.Command)
	rf.mu.Unlock()

	rf.applyCh <- msg

	rf.mu.Lock()
	rf.lastApplied = index
	rf.mu.Unlock()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = 0
	rf.votedFor = -1
	emptylog := logEntry{}
	emptylog.Command = ""
	emptylog.Term = 0
	rf.log = append(rf.log, emptylog)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower
	rf.heartbeatChan = make(chan int)
	rf.electionLoseChan = make(chan int)

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// DPrintf("make peers %d", rf.me)
	
	go rf.run()
	go rf.applyCommitedMsg()
	return rf
}

func (rf *Raft) run() {
	r := rand.New(rand.NewSource(int64(rf.me * 100)))
	for {
		if rf.killed() {
			// fmt.Println(rf.me, "is killed")
			return
		}
		rv := r.Intn(150) + 150
		ret := rf.electionTimer(rv)
		if ret {
			DPrintf("%d start election on term %d after %d", rf.me, rf.currentTerm + 1, rv)
			win := rf.election()
			if win {
				// go rf.AEProcess()
				go rf.updateCommitIndex()
				rf.AEProcess()
				// rf.heartbeats()
			}
		}
	}
}

func (rf *Raft) electionTimer(t int) bool {
	i := 0
	for {
		if rf.killed() {
			return false
		}
		for {
			select {
			case <-rf.heartbeatChan:
				i = 0
				// fmt.Println(seed, "reset election timer to", rv)
				break
			default:
				time.Sleep(time.Millisecond)
				i++
				if i == t {
					// fmt.Println(rf.me, "timer", rv, "is up")
					return true
				}
			}
		}
	}
}

func (rf *Raft) election() bool {
	// fmt.Println(rf.me, "start election")
	if rf.killed() {
		return false
	}

	// fmt.Println(rf.me, "in election")
	electionTimeoutChan := make(chan int)
	go func(c chan int) {
		time.Sleep(200 * time.Millisecond)
		c <- 1
	}(electionTimeoutChan)

	rf.mu.Lock()

	// fmt.Println(rf.me, "get lock")
	currentTerm := rf.currentTerm + 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.count = 1

	waitonRequestVoteReplyChan := make(chan int)

	// rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term

	rf.mu.Unlock()

	// fmt.Println(rf.me, "in election")
	cond := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// fmt.Println("send out request vote msg from", rf.me, "to", i, "term", args.Term, "state", rf.state)
		go func(server int, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, args, &reply)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
			}
			if reply.VoteGranted {
				// fmt.Println(rf.me, "receive vote from", server)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.count++
				cond.Broadcast()
			}
		}(i, &args)
	}

	go func(c chan int) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for rf.count < len(rf.peers) / 2 + 1 {
			cond.Wait()
		}
		if rf.count >= len(rf.peers) / 2 + 1 {
			// fmt.Println(rf.me, "got majority vote")
			c <- 1
			close(c)
			return
		} else {
			return
		}
	}(waitonRequestVoteReplyChan)
	

	for {
		select {
		case <-waitonRequestVoteReplyChan:
			DPrintf("%d is Leader now on term %d", rf.me, rf.currentTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.state = Leader
			rf.votedFor = -1
			rf.currentTerm = currentTerm
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log);
				rf.matchIndex[i] = 0;
				DPrintf("leader %d update follower %d nextIndex to %d", rf.me, i, rf.nextIndex[i])
			}

			return true
			// leader
		case <-rf.electionLoseChan: //receive AE
			// if rf.state == Candidate {
			// 	fmt.Println(rf.me, "reveive heartbeat signal, back to Follower")
			// }
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.state = Follower
			rf.votedFor = -1

			return false
		case <-electionTimeoutChan: //timeout
			// if rf.state == Candidate {
				// fmt.Println(rf.me, "didn't get enough vote, back to Follower")
			// }
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.state = Follower
			rf.votedFor = -1

			return false
		}
	}
}

// func (rf *Raft) heartbeats() {
// 	for {
// 		if rf.killed() {
// 			return
// 		}

// 		rf.mu.Lock()
// 		if rf.state == Leader {
// 			// fmt.Println(rf.me, "send out heartbeats to peers")
// 			for i := 0; i < len(rf.peers); i++ {
// 				if i == rf.me {
// 					continue
// 				}

// 				args := AppendEntriesArgs{}
// 				args.Term = rf.currentTerm
// 				args.LeaderId = rf.me
// 				args.PrevLogIndex = rf.nextIndex[i] - 1
// 				args.PrevLogTerm = rf.log[rf.nextIndex[i] - 1].Term
// 				args.LeaderCommit = rf.commitIndex
				
// 				go rf.sendHeartbeat(i, &args)
// 			}
// 		} else {
// 			// fmt.Println(rf.me, "is not leader anymore")
// 			rf.mu.Unlock()
// 			return
// 		}
// 		rf.mu.Unlock()

// 		time.Sleep(100 * time.Millisecond)
// 	}
// }


func (rf *Raft) receiveHeartbeat(server int) {
	// fmt.Println(rf.me, "receive heartbeat from", server)
	rf.heartbeatChan <- server
}

func (rf *Raft) breakElection() {
	// fmt.Println(rf.me, "break election", rf.state, "term", rf.currentTerm)
	rf.electionLoseChan <- 1
}

func (rf *Raft) AEProcess() {
	// DPrintf("%d start AEProcess", rf.me)
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// DPrintf("%d start AE process on term %d", rf.me, rf.currentTerm)
		next := len(rf.log)
		term := rf.currentTerm
		DPrintf("%d in AEProcess %d on term %d", rf.me, next, rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			// DPrintf("%d send AE to %d", rf.me, i)
			go rf.sendAppendEntries(i, term, next)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	var start int 
	var end int
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		start = rf.commitIndex + 1
		end = len(rf.log) - 1
		// DPrintf("%d updateCommitIndex, start %d, end %d", rf.me, start, end)
		var mid int
		var cnt int
		for start <= end {
			mid = start + (end - start) / 2
			cnt = 0 // count self
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= mid {
					cnt++
				}
			}
			if cnt >= len(rf.peers) / 2 + 1 {
				start = mid + 1
			} else {
				end = mid - 1
			}
		}

		// DPrintf("%d finish updateCommitIndex, start %d, end %d", rf.me, start, end)
		if end > rf.commitIndex && rf.log[end].Term == rf.currentTerm {
			rf.commitIndex = end
			DPrintf("%d update commitIndex to %d", rf.me, end)
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) applyCommitedMsg() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			go rf.sendApplyMsg(rf.lastApplied + 1)
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}
