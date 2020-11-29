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
import "bytes"
import "../labgob"

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

	// volatile state for leader election
	state int
	count int
	vote int
	heartbeatChan chan int
	electionLoseChan chan int

	applyCh chan ApplyMsg
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf(" %d save persiste state to disk", rf.me)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil {
	  // error...
	  // DPrintf("%d has error in decode persist", rf.me)
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = logs
	}
	// DPrintf("%d read persist state", rf.me)
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
	rf.readPersist(rf.persister.ReadRaftState())
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	// check if candidate's log is at least as up-to-date as receiver's log
	uptodate := false
	lastindex := len(rf.log) - 1
	lastterm := rf.log[lastindex].Term
	if (args.LastLogTerm > lastterm) || (args.LastLogTerm == lastterm && args.LastLogIndex >= lastindex) {
		uptodate = true
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || rf.state != Follower {
		DPrintf(" %d on state %v refuse vote to %d", rf.me, rf.state, args.CandidateId)
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.Term > rf.currentTerm) && uptodate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf(" %d give vote to %d", rf.me, args.CandidateId)
		go rf.receiveHeartbeat(args.CandidateId) // reset election timer
	} else {
		DPrintf(" %d on state %d, term %d, votedfor %d, refuse vote to %d", rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId)
		DPrintf(" lastindex %d, lastterm %d, requestor %d lastindex %d, lastterm %d", lastindex, lastterm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	}
	rf.mu.Unlock()

	rf.persist()
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
	PrevTerm int // optimize for reducing number of rejected AppendEntries RPC call
	PrevIndex int // optimize for reducing number of rejected AppendEntries RPC call
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.readPersist(rf.persister.ReadRaftState())
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.PrevTerm = 0
	reply.PrevIndex = 0
	
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf(" %d got AE RPC request from %d of term %d, but is on term %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // log inconsistency
		if len(rf.log) <= args.PrevLogIndex {
			reply.PrevIndex = len(rf.log) - 1
			reply.PrevTerm = rf.log[reply.PrevIndex].Term
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			i := args.PrevLogIndex
			t := rf.log[i].Term
			for i > 0 {
				if rf.log[i].Term != t {
					break
				}
				i--
			}
			reply.PrevIndex = i
			reply.PrevTerm = rf.log[i].Term
		}
		DPrintf(" %d got AE RPC from %d with PrevLogIndex %d and PrevLogTerm %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		DPrintf(" %d reply AE RPC with prevIndex %d, prevTerm %d", rf.me, reply.PrevIndex, reply.PrevTerm)
		reply.Success = false
	} else {
		go rf.receiveHeartbeat(args.LeaderId) // reset election timer
		if rf.state == Candidate {
			go rf.breakElection() // lose election, back to candidate
		}

		reply.Success = true

		var j = args.PrevLogIndex + 1
		// DPrintf("%d start append log entry at %d", rf.me, j)
		for i := 0; i < len(args.Entries); i++ {
			if j < len(rf.log) {
				rf.log = rf.log[:j]
			}
			rf.log = append(rf.log, args.Entries[i])
			j++
		}
		if len(args.Entries) == 0 { // heartbeat
			DPrintf(" %d finish heartbeat", rf.me)
		} else { // AppendEntries
			DPrintf(" %d finish append log entry %d", rf.me, j - 1)
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(j - 1)))
			DPrintf(" %d commit index update to %d, log len is %d", rf.me, rf.commitIndex, len(rf.log))
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
	rf.mu.Unlock()

	rf.persist()
}

//
// example code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, term int, len int) bool {
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
	DPrintf(" %d send AE RPC to %d", rf.me, server)
	DPrintf(" term %d, prevLogIndex %d, PrevLogTerm %d, LeaderCommit %d", args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
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
	DPrintf(" %d reply with %v on term %d for AE RPC from %d", server, reply.Success, reply.Term, rf.me)
	if rf.state != Leader {
		return false
	}

	if reply.Success {
		rf.nextIndex[server] = len
		rf.matchIndex[server] = len - 1
		DPrintf(" %d match %d index to %d", rf.me, server, rf.matchIndex[server])
		// rf.mu.Unlock()
		return true
	} else if reply.Term > rf.currentTerm {
		DPrintf(" %d got reply from %d with term %d, convert to follower", rf.me, server, reply.Term)
		rf.currentTerm = reply.Term
		rf.state = Follower

		rf.mu.Unlock()
		rf.persist()
		rf.mu.Lock()
		// rf.mu.Unlock()
		return false
	} else { //reply false, reply.Term <= rf.currentTerm, rf.nextIndex[server] > 0
		if rf.nextIndex[server] >= reply.PrevIndex + 1 {
			rf.nextIndex[server] = reply.PrevIndex + 1
		} else if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		} else {
			rf.nextIndex[server] = 1
		}
		DPrintf(" %d reduce %d nextIndex to %d", rf.me, server, rf.nextIndex[server])
		return false
	}

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
	isLeader = rf.state == Leader
	rf.mu.Unlock()

	if isLeader {
		DPrintf(" %d start send command %v on term %d", rf.me, command, rf.currentTerm)
		rf.mu.Lock()
		index = len(rf.log)
		term = rf.currentTerm
		entry := logEntry{
			Command: command,
			Term: rf.currentTerm,
		}
		rf.log = append(rf.log, entry)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.mu.Unlock()

		rf.persist()
	}

	return index, term, isLeader
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
	// append an emptylog as the 0th logentry
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

	rand.Seed(int64(rf.me * 100))

	// initialize from state persisted before a crash
	rf.mu.Unlock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()

	// DPrintf(" make peers %d", rf.me)
	
	go rf.run()
	go rf.applyCommitedMsg()
	return rf
}

func (rf *Raft) run() {
	for {
		if rf.killed() {
			return
		}
		ret := rf.electionTimer()
		if ret {
			// DPrintf(" %d start election on term %d", rf.me, rf.currentTerm + 1)
			win := rf.election()
			if win {
				go rf.updateCommitIndex()
				rf.AEProcess()
			}
		}
	}
}

func (rf *Raft) electionTimer() bool {
	t := rand.Intn(250) + 150
	i := 0
	for {
		if rf.killed() {
			return false
		}
		for {
			select {
			case <-rf.heartbeatChan:
				i = 0
				// DPrintf(" %d receive heatbeat, break election timer", rf.me)
				t = rand.Intn(250) + 150
				break
			default:
				time.Sleep(time.Millisecond)
				i++
				if i == t {
					DPrintf(" %d start election on term %d after timer %d", rf.me, rf.currentTerm + 1, t)
					return true
				}
			}
		}
	}
}

func (rf *Raft) election() bool {
	if rf.killed() {
		return false
	}

	electionTimeoutChan := make(chan int)
	go func(c chan int) {
		t := rand.Intn(50) + 150
		time.Sleep(time.Duration(t) * time.Millisecond)
		c <- 1
	}(electionTimeoutChan)

	rf.mu.Lock()

	currentTerm := rf.currentTerm + 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.count = 1
	rf.vote = 1

	args := RequestVoteArgs{}
	args.Term = currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term

	rf.mu.Unlock()

	rf.persist()

	cond := sync.NewCond(&rf.mu)
	id := rand.Int() % 10000 + 100000
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			DPrintf(" %d send out request vote RPC of id %d to %d for term %d", rf.me, id, server, args.Term)
			ok := rf.sendRequestVote(server, args, &reply)
			rf.mu.Lock()
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					rf.persist()
					rf.mu.Lock()
				}
				DPrintf(" %d receive vote of id %d from %d of %v ", rf.me, id, server, reply.VoteGranted)
				if rf.state == Candidate && reply.VoteGranted {
					rf.vote++
				}
			}
			rf.count++
			cond.Broadcast()
			rf.mu.Unlock()
		}(i, &args)
	}

	waitonRequestVoteReplyChan := make(chan int)
	loseElectionChan := make(chan int)

	go func(c1 chan int, c2 chan int) {
		rf.mu.Lock()

		for rf.vote < len(rf.peers) / 2 + 1 && rf.count < len(rf.peers) {
			cond.Wait()
		}
		if rf.vote >= len(rf.peers) / 2 + 1 {
			rf.mu.Unlock()
			c1 <- 1
			close(c1)
			close(c2)
			return
		} else {
			rf.mu.Unlock()
			c2 <- 1
			close(c1)
			close(c2)
			return
		}
	}(waitonRequestVoteReplyChan, loseElectionChan)
	
	DPrintf(" %d start waiting on election result on term %d", rf.me, currentTerm)
	for {
		select {
		case <-waitonRequestVoteReplyChan:
			DPrintf(" %d is Leader now on term %d", rf.me, currentTerm)
			rf.mu.Lock()
			// defer rf.mu.Unlock()

			rf.state = Leader
			rf.votedFor = -1
			rf.currentTerm = currentTerm
			rf.count = 0
			rf.vote = 0
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log);
				rf.matchIndex[i] = 0;
				DPrintf(" leader %d update follower %d nextIndex to %d", rf.me, i, rf.nextIndex[i])
			}
			rf.mu.Unlock()

			rf.persist()
			return true
			// leader
		case <-loseElectionChan:
			DPrintf(" %d didn't get majority vote for term %d", rf.me, currentTerm)
			rf.mu.Lock()

			rf.state = Follower
			rf.votedFor = -1
			rf.count = 0
			rf.vote = 0
			rf.mu.Unlock()

			rf.persist()
			return false
		case <-rf.electionLoseChan: //receive AE
			DPrintf(" %d receive heartbeat signal, lost election for term %d", rf.me, currentTerm)
			rf.mu.Lock()
			// defer rf.mu.Unlock()

			rf.state = Follower
			rf.votedFor = -1
			rf.count = 0
			rf.vote = 0
			rf.mu.Unlock()

			rf.persist()
			return false
		case <-electionTimeoutChan: //timeout
			DPrintf(" %d didn't get enough vote int time for term %d, back to Follower", rf.me, currentTerm)
			rf.mu.Lock()
			// defer rf.mu.Unlock()

			rf.state = Follower
			rf.votedFor = -1
			rf.count = 0
			rf.vote = 0
			rf.mu.Unlock()

			rf.persist()

			return false
		}
	}
}

func (rf *Raft) receiveHeartbeat(server int) {
	rf.heartbeatChan <- server
}

func (rf *Raft) breakElection() {
	rf.electionLoseChan <- 1
}

// leader runs continuously to send AppendEntries or heartbeat to followers
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
		DPrintf(" %d in AEProcess %d on term %d", rf.me, next, rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			// DPrintf("%d send AE to %d", rf.me, i)
			go rf.sendAppendEntries(i, term, next)
		}
		rf.mu.Unlock()

		time.Sleep(150 * time.Millisecond)
	}
}

// leader runs continuously to update commit index
// use binary search to check newly commited index
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
		DPrintf(" %d updateCommitIndex, start %d, end %d", rf.me, start, end)
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
			DPrintf(" %d update commitIndex to %d", rf.me, end)
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

// all servers runs continuiously to check if commit index > last applied index
// once commit index > last applied index, apply the committed cmd to state machine
func (rf *Raft) applyCommitedMsg() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.mu.Unlock()
			rf.sendApplyMsg(rf.lastApplied + 1)
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		
	}
}

// apply command to state machine
func (rf *Raft) sendApplyMsg(index int) {
	// DPrintf("%d send apply msg %v to applyCh", rf.me, index)
	
	rf.mu.Lock()
	msg := ApplyMsg{
		CommandValid: true,
		Command: rf.log[index].Command,
		CommandIndex: index,
	}
	DPrintf(" %d send apply msg %v of %d to applyCh", rf.me, index, msg.Command)
	rf.mu.Unlock()

	rf.applyCh <- msg

	rf.mu.Lock()
	rf.lastApplied = index
	rf.mu.Unlock()
}
