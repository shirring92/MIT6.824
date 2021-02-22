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
import "log"

const (
	Leader = 1
	Follower = 2
	Candidate = 3
)

const NShards = 10

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
	CommandTerm  int
	Snapshot 	 Snapshot
}

type Record struct {
	MsgIndex int
	MsgTerm int
	Execute bool
}

type logEntry struct {
	Command interface{}
	Term int
}

type Snapshot struct {
	LastAppliedIndex int
	LastAppliedTerm int
	KVstorage map[string]string
	ClientLastReq map[string]int
	ReceivedIndex map[int]Record
	Shards [NShards]Shard
}

type Shard struct {
	Num int
	Gid int
	LastCfg int
	Status bool
	KVstorage map[string]string
	Dupmap map[int]bool
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

	// persistent state on all servers
	currentTerm int
	votedFor int
	log []logEntry
	logOffset int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	// volatile state for leader election
	// leaderId int
	state int
	count int
	vote int
	heartbeatChan chan int
	electionLoseChan chan int
	// electionTimeoutChan chan int

	applyCh chan ApplyMsg
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
	// rf.mu.Lock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)
	data := w.Bytes()

	// rf.mu.Unlock()

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
	// this function is only called by Make, and it can use the lock inside Make
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	var logOffset int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil ||
	   d.Decode(&logOffset) != nil {
	  // error...
	  DPrintf("%d has error in decode persist", rf.me)
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = logs
	  rf.logOffset = logOffset
	}
	// DPrintf("%d read persist state", rf.me)
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
	// rf.mu.Unlock()

	if isLeader {
		// DPrintf(" %d start send command %v on term %d", rf.me, command, rf.currentTerm)
		// rf.mu.Lock()
		index = len(rf.log) + rf.logOffset
		term = rf.currentTerm
		entry := logEntry{
			Command: command,
			Term: rf.currentTerm,
		}
		rf.log = append(rf.log, entry)
		rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.logOffset
		DPrintf(" %d got cmd %d on term %d: %v", rf.me, index, term, command)
		rf.persist()
		// DPrintf(" %d save persist state on Start: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
	}

	return index, term, isLeader
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
	// rf.readPersist(rf.persister.ReadRaftState())
	// DPrintf(" %d read persist state on RequestVote: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default value
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// DPrintf(" %d on state %v refuse vote to %d", rf.me, rf.state, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// if rf.state == Candidate { // if raft is in candidate state and current term is smaller, break election, back to follower
		// 	go rf.breakElection(rf.currentTerm)
		// }
		rf.state = Follower
		// DPrintf(" %d update currentTerm to %d on RV RPC from %d", rf.me, rf.currentTerm, args.CandidateId)
	}

	// check if candidate's log is at least as up-to-date as receiver's log
	uptodate := false
	logIndex := len(rf.log) - 1
	lastindex := logIndex + rf.logOffset
	lastterm := 0
	if lastindex >= 0 {
		lastterm = rf.log[logIndex].Term
	}
	if (args.LastLogTerm > lastterm) || (args.LastLogTerm == lastterm && args.LastLogIndex >= lastindex) {
		uptodate = true
	} else {
		// DPrintf(" %d refuse RV request from %d, log conflic", rf.me, args.CandidateId)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// DPrintf(" %d give vote to %d", rf.me, args.CandidateId)
		go rf.receiveHeartbeat(args.CandidateId) // reset election timer
	}

	rf.persist()
	// DPrintf(" %d save persist state on RequestVote: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
	// rf.mu.Unlock()
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
	ConflicTerm int // optimize for reducing number of rejected AppendEntries RPC call
	ConflicIndex int // optimize for reducing number of rejected AppendEntries RPC call
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rf.readPersist(rf.persister.ReadRaftState())
	// DPrintf(" %d read persist state on AppendEntries: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default value
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflicTerm = -1
	reply.ConflicIndex = len(rf.log) + rf.logOffset
	
	// DPrintf(" %d append entries: log length %d, logOffset %d, prevLogIndex %d, prevLogTerm %d", len(rf.log), rf.logOffset, args.PrevLogIndex, args.PrevLogTerm)
	if args.Term < rf.currentTerm {
		reply.Success = false
		// DPrintf(" %d got AE RPC request from %d of term %d, but is on term %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm {
		// DPrintf(" %d will reset election timer", rf.me)
		go rf.receiveHeartbeat(args.LeaderId) // reset election timer
	}
	// DPrintf(" %d will reset election timer: %v", rf.me, reset)
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.state = Follower
		}
		if rf.votedFor != -1 {
			rf.votedFor = -1
		}

		// rf.mu.Unlock()
		rf.persist()
		// DPrintf(" %d save persist state on AppendEntries: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
		// rf.mu.Lock()
	}

	if args.PrevLogIndex < rf.logOffset{
		reply.Success = false
		reply.ConflicIndex = rf.logOffset
		reply.ConflicTerm = rf.log[0].Term
		// DPrintf(" %d have logOffset %d > args.PrevLogIndex %d in AE RPC from %d", rf.me, rf.logOffset, args.PrevLogIndex, args.LeaderId)
		return
	}
	if args.PrevLogIndex < len(rf.log) + rf.logOffset {
		if rf.log[args.PrevLogIndex - rf.logOffset].Term != args.PrevLogTerm {
			reply.Success = false
			i := args.PrevLogIndex - rf.logOffset
			t := rf.log[i].Term
			reply.ConflicTerm = t
			for i >= 0 && rf.log[i].Term == t {
				i--
			}
			reply.ConflicIndex = i + 1 + rf.logOffset
			// DPrintf(" %d prevLogTerm %d at index %d not match args.PrevLogTerm %d in AE RPC", rf.me, rf.log[args.PrevLogIndex - rf.logOffset].Term, args.PrevLogIndex, args.PrevLogTerm)
			// DPrintf(" %d return conflic index %d, conflic term %d to %d", rf.me, reply.ConflicIndex, reply.ConflicTerm, args.LeaderId)
			return
		}

		reply.Success = true
		var j = args.PrevLogIndex - rf.logOffset + 1 // corresponding index of first entry in args.entries in rf.log
		for i := 0; i < len(args.Entries); i++ {
			if i + j >= len(rf.log) || (i + j < len(rf.log) && rf.log[i + j].Term != args.Entries[i].Term) {
				rf.log = rf.log[:i + j]
				for e := i; e < len(args.Entries); e++ {
					rf.log = append(rf.log, args.Entries[e])
				}
				break
			}

		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log) + rf.logOffset - 1)))
			// DPrintf(" %d commit index update to %d, log len is %d", rf.me, rf.commitIndex, len(rf.log))
		}
		rf.persist()
		// DPrintf(" %d save persist state on AppendEntries success from %d: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, args.LeaderId, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
		// rf.mu.Lock()
		return
	}
	if args.PrevLogIndex >= len(rf.log) + rf.logOffset {
		reply.Success = false
		reply.ConflicIndex = len(rf.log) + rf.logOffset
		reply.ConflicTerm = -1
		// DPrintf(" %d does not have log entry in prevLogIndex %d in AE RPC from %d", rf.me, args.PrevLogIndex, args.LeaderId)
		return
	}

	// rf.persist()
	// DPrintf(" %d save persist state on AppendEntries: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
}

//
// example code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int) bool {
	rf.mu.Lock()

	length := len(rf.log) + rf.logOffset
	term := rf.currentTerm

	previdx := rf.nextIndex[server] - 1
	if previdx < rf.logOffset {
		previdx = rf.logOffset
	}
	// DPrintf(" %d send AE RPC to %d, nextIndex is %d, previdx is %d, logOffset is %d, log length is %d", rf.me, server, rf.nextIndex[server], previdx, rf.logOffset, len(rf.log))
	args := AppendEntriesArgs{
		Term: term,
		LeaderId: rf.me,
		PrevLogIndex: previdx,
		PrevLogTerm: rf.log[previdx - rf.logOffset].Term,
		LeaderCommit: rf.commitIndex,
	}
	for i := previdx + 1; i < length; i++ {
		args.Entries = append(args.Entries, rf.log[i - rf.logOffset])
	}
	// DPrintf(" term %d, prevLogIndex %d, PrevLogTerm %d, LeaderCommit %d", args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	DPrintf(" %d send AE args <term %d, leaderid %d, prevLogIndex %d, prevLogTerm %d, LeaderCommit %d> to %d", rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, server)

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// DPrintf("%d fail to connect with %d on AE RPC on term %d, state is %v", rf.me, server, args.Term, rf.state)
		// time.Sleep(30 * time.Millisecond)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf(" %d reply with %v on term %d for AE RPC from %d", server, reply.Success, reply.Term, rf.me)
	// if rf.state != Leader {
	// 	return false
	// }
	DPrintf(" %d receive AE reply %v from %d corresponding to args <term %d, leaderid %d, prevLogIndex %d, prevLogTerm %d, LeaderCommit %d>", rf.me, reply, server, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	if args.Term != rf.currentTerm {
		// DPrintf(" %d receive a delayed RPC response. send AE args in term %d, currentTerm %d", rf.me, args.Term, rf.currentTerm)
		return false
	}

	if reply.Term > rf.currentTerm {
		// DPrintf(" %d got reply from %d with term %d, convert to follower", rf.me, server, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower

		// rf.mu.Unlock()
		rf.persist()
		// DPrintf(" %d save persist state on send AppendEntries reply: currentTerm %d, votedFor %d, log len %d, logOffset %d, back to follower", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
		// rf.mu.Lock()
		// rf.mu.Unlock()
		return false
	}

	if reply.Success {
		rf.nextIndex[server] = length
		// DPrintf(" leader %d update follower %d nextIndex to %d on AE reply", rf.me, server, rf.nextIndex[server])
		rf.matchIndex[server] = length - 1
		DPrintf(" %d match %d next index to %d", rf.me, server, rf.nextIndex[server])
		// rf.mu.Unlock()
		if rf.commitIndex < length - 1 {
			rf.updateCommitIndex(length - 1)
		}
		return true
	}

	if reply.ConflicTerm > 0 && reply.ConflicTerm <= rf.log[len(rf.log) - 1].Term{
		var i = len(rf.log) - 1
		for i >= 0 {
			if rf.log[i].Term == reply.ConflicTerm {
				i++
				break
			} else {
				i--
			}
		}
		if i < 0 {
			rf.nextIndex[server] = reply.ConflicIndex
		} else if i >= len(rf.log) {
			rf.nextIndex[server] = reply.ConflicIndex
		} else {
			rf.nextIndex[server] = i + rf.logOffset
		}
	} else {
		rf.nextIndex[server] = reply.ConflicIndex
	}
	// DPrintf(" %d receive conflict log reply from %d with conflic <index %d, term %d>, reduce nextIndex to %d", rf.me, server, reply.ConflicIndex, reply.ConflicTerm, rf.nextIndex[server])

	return false
}

//
// example InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
//
type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Snapshot Snapshot
}

//
// example InstallSnapshot RPC reply structure.
// field names must start with capital letters!
//
type InstallSnapshotReply struct {
	Term int
}

//
// example InstallSnapshot RPC handler.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// rf.readPersist(rf.persister.ReadRaftState())
	// DPrintf(" %d read persist state on InstallSnapshot: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)

	rf.mu.Lock()

	reply.Term = rf.currentTerm
	DPrintf(" %d receive InstallSnapshot %v", rf.me, args)
	// DPrintf(" args term is from %d", args.LeaderId)
	if args.Term < rf.currentTerm {
		// DPrintf(" %d currentTerm is %d, snapshot from leader %d term is %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	} else {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			// DPrintf(" %d save persist state on InstallSnapshot: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
		}
		// if args.LastIncludedIndex <= len(rf.log) + rf.logOffset {
		// 	DPrintf(" %d already apply snapshot of index %d, current index is %d", rf.me, args.LastIncludedIndex, len(rf.log) -1 + rf.logOffset)
		// } else {

		// create new snapshot 
		// discard existing snapshot
		// discard log enties precede snapshot
		// reset state machine
			go rf.receiveHeartbeat(args.LeaderId)
			// DPrintf(" %d has logIndex %d, last Index %d, last Term %d, snapshot %v", rf.me, logIndex, args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
			// if args.LastIncludedIndex > rf.logOffset { // check this condition inside sub-functions, in case when sub-function got lock, offset already changed
			go rf.saveSnapshot(args.Snapshot, args.LastIncludedIndex, args.LastIncludedTerm)
			go rf.sendSnapshotToServer(args.Snapshot, args.LastIncludedIndex, args.LastIncludedTerm)
			// }
	}
	rf.mu.Unlock()
}

//
// example code to send a InstallSnapshot RPC to a server.
//
func (rf *Raft) sendInstallSnapshot(server int, i int, t int, s Snapshot) bool {
		rf.mu.Lock()
		args := InstallSnapshotArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LastIncludedIndex: i,
			LastIncludedTerm: t,
			Snapshot: s,
		}
		
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
		
	DPrintf(" %d sendInstallSnapshot %v to %d", rf.me, args, server)
	// DPrintf(" term %d, leaderid %d, lastIndex %d, lastTerm %d", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower

		// rf.mu.Unlock()
		rf.persist()
		// DPrintf(" %d save persist state on send InstallSnapshot reply: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
		// rf.mu.Lock()
	} else {
		if rf.matchIndex[server] < i {
			rf.matchIndex[server] = i
			rf.nextIndex[server] = i + 1
			// DPrintf(" leader %d update follower %d nextIndex to %d on IS reply", rf.me, server, rf.nextIndex[server])
		}
		
	}
	rf.mu.Unlock()

	return true
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
	// labgob.Register(Record{})
	// labgob.Register(Snapshot{})

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
	rf.logOffset = 0
	
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// rf.leaderId = -1
	rf.state = Follower
	rf.heartbeatChan = make(chan int)
	rf.electionLoseChan = make(chan int)
	// rf.electionTimeoutChan = make(chan int)

	rf.applyCh = applyCh
	
	rand.Seed(int64(rf.me * 100))

	// rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// rf.mu.Lock()
	DPrintf(" %d read persist state on Make: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
	rf.commitIndex = rf.logOffset
	rf.lastApplied = rf.logOffset
	// rf.mu.Lock()

	DPrintf(" make raft %d", rf.me)
	
	go rf.run()
	// go rf.electionTimer()
	go rf.applyCommitedMsg()
	return rf
}

// func (rf *Raft) run() {
// 	// DPrintf(" %d start run function", rf.me)
// 	for {
// 		if rf.killed() {
// 			return
// 		}
// 		ret := rf.electionTimer()
// 		// DPrintf(" %d finish election time out: %v", rf.me, ret)
// 		if ret {
// 			// for rf.state == Candidate {
// 				if rf.killed() {
// 					return
// 				}
// 				win := rf.election()
// 				if win {
// 					go rf.updateCommitIndex()
// 					rf.AEProcess()
// 				}
// 			// }
// 		}
// 	}
// }

func (rf *Raft) run() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == Follower {
			rf.followerTimer()
		} else if state == Candidate {
			rf.election()
		} else {
			rf.AEProcess()
		}
	}
}

// func (rf *Raft) electionTimer() bool {
// 	t := rand.Intn(250) + 150
// 	// DPrintf(" %d start timer %d", rf.me, t)
// 	i := 0
// 	for {
// 		if rf.killed() {
// 			return false
// 		}
// 		// for {
// 			// DPrintf(" %d timer is %d", rf.me, i)
// 			select {
// 			case <-rf.heartbeatChan:
// 				i = 0
// 				DPrintf(" %d receive heatbeat, break election timer", rf.me)
// 				t = rand.Intn(250) + 150
// 				break
// 			default:
// 				time.Sleep(time.Millisecond)
// 				i++
// 				if i == t {
// 					rf.mu.Lock()
// 					// DPrintf(" %d start election on term %d after timer %d", rf.me, rf.currentTerm + 1, t)
// 					// rf.state = Candidate
// 					rf.mu.Unlock()
// 					return true
// 				}
// 			}
// 		// }
// 	}
// }
func (rf *Raft) followerTimer() {
	t := rand.Intn(150) + 250
	i := 0
	// DPrintf(" %d in follower state, start election timer %d", rf.me, t)
	for {
		if rf.killed() {
			return
		}
		select {
		case <- rf.heartbeatChan: // receive heartbeat, reset timer
			i = 0
			t = rand.Intn(150) + 250
			time.Sleep(time.Millisecond)
			// DPrintf(" %d receive heartbeat in follower state, reset election timer %d", rf.me, t)
		default:
			time.Sleep(time.Millisecond)
			i++
			if i == t { // time up, start new election
				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
				return
			}
		}
	}
}

func (rf *Raft) election() bool {
	DPrintf(" %d start election on term %d", rf.me, rf.currentTerm + 1)
	electionTimeoutChan := make(chan int)
	go func(c chan int) {
		t := rand.Intn(150) + 250
		time.Sleep(time.Duration(t) * time.Millisecond)
		// DPrintf(" %d election timer %v, %v", rf.me, t, s)
		c <- 1
	}(electionTimeoutChan)

	rf.mu.Lock()

	currentTerm := rf.currentTerm
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.count = 1
	rf.vote = 1

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1 + rf.logOffset
	if args.LastLogIndex < 0 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.log[args.LastLogIndex - rf.logOffset].Term
	}

	rf.persist()
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)
	// id := rand.Int() % 10000 + 100000
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			// DPrintf(" %d send out request vote RPC of id %d to %d for term %d", rf.me, id, server, args.Term)
			ok := rf.sendRequestVote(server, args, &reply)
			rf.mu.Lock()

			if ok {
				if args.Term != rf.currentTerm {
					// DPrintf(" %d receive a delayed RV reply of id %d from %d", rf.me, id, server)
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					// rf.mu.Unlock()
					rf.persist()
					// DPrintf(" %d receive higher term %d on RV reply from %d, back to follower", rf.me, rf.currentTerm, i)
					// rf.mu.Lock()
				} else if rf.state == Candidate && reply.VoteGranted {
					// DPrintf(" %d receive vote of id %d from %d", rf.me, id, server)
					rf.vote++
				}
			}
			rf.count++
			cond.Broadcast()
			rf.mu.Unlock()
		}(i, &args)
	}

	waitonRequestVoteReplyChan := make(chan bool)
	// loseElectionChan := make(chan int)

	go func(c1 chan bool) {
		rf.mu.Lock()

		for rf.vote < len(rf.peers) / 2 + 1 && rf.count < len(rf.peers) {
			cond.Wait()
		}
		if rf.vote >= len(rf.peers) / 2 + 1 {
			rf.mu.Unlock()
			c1 <- true
			close(c1)
			// close(c2)
			return
		} else {
			rf.mu.Unlock()
			c1 <- false
			close(c1)
			// close(c2)
			return
		}
	}(waitonRequestVoteReplyChan)
	
	// waitResult:
	for {
		if rf.killed() {
			return false
		}
		if rf.state != Candidate {
			// DPrintf(" %d is not Candidate, exit election", rf.me)
			return false
		}
		// DPrintf(" %d start waiting on election result on term %d", rf.me, currentTerm)
		select {
		case enoughVote := <-waitonRequestVoteReplyChan:
			
			rf.mu.Lock()
			// defer rf.mu.Unlock()

			// rf.leaderId = rf.me
			if enoughVote {
				DPrintf(" %d is Leader now on term %d", rf.me, rf.currentTerm)
				rf.state = Leader
				// rf.votedFor = -1
				// rf.currentTerm = currentTerm
				// rf.count = 0
				// rf.vote = 0
				// emptylog := logEntry{
				// 	Command: "",
				// 	Term: currentTerm,
				// }
				// rf.log = append(rf.log, emptylog)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) + rf.logOffset;
					rf.matchIndex[i] = 0;
					DPrintf(" leader %d update follower %d nextIndex to %d, matchIndex to %d on election success", rf.me, i, rf.nextIndex[i], rf.matchIndex[i])
				}
				rf.matchIndex[rf.me] = len(rf.log) + rf.logOffset - 1;
				// go rf.updateCommitIndex()
				// go rf.AEProcess()
			} else {
				// DPrintf(" %d didn't get majority vote for term %d", rf.me, currentTerm)
				rf.state = Follower
				rf.currentTerm = currentTerm
			}
			rf.votedFor = -1
			rf.count = 0
			rf.vote = 0
			rf.persist()
			rf.mu.Unlock()

			
			return enoughVote
			// leader
		// case <-loseElectionChan:
		// 	// DPrintf(" %d didn't get majority vote for term %d", rf.me, currentTerm)
		// 	rf.mu.Lock()

		// 	// rf.leaderId = -1
		// 	rf.state = Follower
		// 	rf.votedFor = -1
		// 	rf.count = 0
		// 	rf.vote = 0
		// 	rf.mu.Unlock()

		// 	rf.persist()
		// 	return false
		// case t := <-rf.electionLoseChan: //receive AE or RV with higher term
		// 	DPrintf(" %d receive heartbeat signal, lost election for term %d", rf.me, currentTerm)
		// 	rf.mu.Lock()
		// 	// defer rf.mu.Unlock()

		// 	rf.state = Follower
		// 	rf.currentTerm = int(math.Max(float64(currentTerm), float64(t)))
		// 	rf.votedFor = -1
		// 	rf.count = 0
		// 	rf.vote = 0
		// 	rf.persist()
		// 	rf.mu.Unlock()

		// 	// rf.persist()
		// 	return false
		case <-electionTimeoutChan: //timeout
			// DPrintf(" %d didn't get enough vote for election term %d, back to Candidate, start new election", rf.me, rf.currentTerm)
			rf.mu.Lock()
			// defer rf.mu.Unlock()

			rf.state = Candidate
			rf.currentTerm = currentTerm
			rf.votedFor = -1
			rf.count = 0
			rf.vote = 0
			rf.persist()
			rf.mu.Unlock()

			// rf.persist()
			return false
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func (rf *Raft) receiveHeartbeat(server int) {
	rf.heartbeatChan <- server
}

func (rf *Raft) breakElection(term int) {
	rf.electionLoseChan <- term
}

// leader runs continuously to send AppendEntries or heartbeat to followers
func (rf *Raft) AEProcess() {
	// DPrintf("%d start AEProcess", rf.me)
	var t = 0
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		if t == 100 {
			t = 0
		}
		if t == 0 {
			// DPrintf("%d start AE process on term %d", rf.me, rf.currentTerm)
			next := len(rf.log) + rf.logOffset
			DPrintf(" %d in AEProcess %d on term %d, prev log %v", rf.me, next - 1, rf.currentTerm, rf.log[len(rf.log) - 1])
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				// DPrintf("%d send AE to %d", rf.me, i)
				if rf.nextIndex[i] <= rf.logOffset - 1{
					rf.mu.Unlock()
					s, lastindex, lastterm := rf.readSnapshot()
					rf.mu.Lock()
					// DPrintf(" %d send InstallSnapshot to %d with lastindex %d, lastterm %d", rf.me, i, lastindex, lastterm)
					go rf.sendInstallSnapshot(i, lastindex, lastterm, s)
				} else {
					// DPrintf(" %d send AE to %d", rf.me, i)
					go rf.sendAppendEntries(i)
				}
			}
		}
		rf.mu.Unlock()

		t++
		time.Sleep(time.Millisecond)
	}
}

// leader runs continuously to update commit index
// use binary search to check newly commited index
func (rf *Raft) updateCommitIndex(index int) {
		cnt := 0

		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				cnt++
			}
		}
		if cnt >= len(rf.peers) / 2 + 1 && rf.log[index - rf.logOffset].Term == rf.currentTerm {
			rf.commitIndex = index
			DPrintf(" %d update commitIndex to %d, lastapplied is %d, commit log %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.commitIndex - rf.logOffset])
		}
}

// all servers runs continuiously to check if commit index > last applied index
// once commit index > last applied index, apply the committed cmd to state machine
func (rf *Raft) applyCommitedMsg() {
	// DPrintf(" %d start run function", rf.me)
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			DPrintf(" %d start apply commited msg %d: %v, lastApplied is %d", rf.me, rf.commitIndex, rf.log[rf.commitIndex - rf.logOffset], rf.lastApplied)
			index := rf.lastApplied + 1
			rf.mu.Unlock()
			rf.sendApplyMsg(index)
			rf.mu.Lock()
			if index > rf.lastApplied {
				if index > rf.logOffset {
					rf.lastApplied = index
				} else {
					rf.lastApplied = rf.logOffset
				}
				
			}
			rf.mu.Unlock()
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
	if index < rf.logOffset || rf.log[index - rf.logOffset].Command == "" {
		// DPrintf(" %d fail to apply cmd %d, logOffset is %d", rf.me, index, rf.logOffset)
		// DPrintf(" %d do not apply empty cmd %d", rf.me, index)
		rf.mu.Unlock()
		return
	}
	var ss Snapshot
	msg := ApplyMsg{
		CommandValid: true,
		Command: rf.log[index - rf.logOffset].Command,
		CommandIndex: index,
		CommandTerm: rf.log[index - rf.logOffset].Term,
		Snapshot: ss,
	}
	DPrintf(" %d start send apply msg %d: %v to applyCh", rf.me, index, msg.Command)
	rf.mu.Unlock()
	
	rf.applyCh <- msg
	DPrintf(" %d finish send apply msg %d: %v to applyCh", rf.me, index, msg.Command)
	
}

// 
// ------------------Lab 3B, snapshot-------------------
//
func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

// kvserver call this function first after restart
func (rf *Raft) GetSnapshot() bool {
	s, i, t := rf.readSnapshot()
	DPrintf(" %d read snapshot %v from persister", rf.me, s)
	if i >= 0 && t >= 0 {
		rf.sendSnapshotToServer(s, i, t)
		return true
	}
	return false
}

func (rf *Raft) readSnapshot() (Snapshot, int, int) {
	data := rf.persister.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		var s Snapshot
		return s, -1, -1
	}

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// DPrintf(" %d read snapshot data %v from persister", rf.me, data)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var term int
	var s Snapshot

	if d.Decode(&index) != nil ||
	   d.Decode(&term) != nil ||
	   d.Decode(&s) != nil {
	   	log.Fatal("decode snapshot error")
	}

	// DPrintf(" %d read snapshot %v from persister on index %d, term %d", rf.me, s, index, term)
	return s, index, term
}

// raft send snapshot to its kvserver through applyCh
func (rf *Raft) sendSnapshotToServer(s Snapshot, i int, t int) {
	// rf.mu.Lock()
	// if i <= rf.logOffset {
	// 	DPrintf(" %d got index %d, but offset is %d, will not send snapshot to server", rf.me, i, rf.logOffset)
	// 	rf.mu.Unlock()
	// 	return
	// }
	// rf.mu.Unlock()
	rf.mu.Lock()
	if i < rf.logOffset {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	msg := ApplyMsg{
		CommandValid: false,
		Command: "",
		CommandIndex: i,
		CommandTerm: t,
		Snapshot: s,
	}
	// DPrintf(" %d start send snapshot msg %v to applyCh", rf.me, msg.Snapshot)
	rf.applyCh <- msg
	DPrintf(" %d done send snapshot msg %v to applyCh", rf.me, msg.Snapshot)
}

//
// save snapshot
// index is the last applied index, term is the last applid term
//
func (rf *Raft) LeaderSaveSnapshot(s Snapshot, index int, term int) bool {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}

	// DPrintf(" %d leader save snapshot %v on last index %d, last term %d, log length %d, log offset %d", rf.me, s, index, term, len(rf.log), rf.logOffset)
	rf.mu.Unlock()
	go rf.saveSnapshot(s, index, term)
	// go rf.discardLog(index, term)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// if index >= rf.nextIndex[i] {
			// rf.nextIndex[i] = index + 1
			// rf.matchIndex[i] = index
			go rf.sendInstallSnapshot(i, index, term, s)
		// }
	}

	return true
}

func (rf *Raft) saveSnapshot(s Snapshot, i int, t int) {
	rf.mu.Lock()
	if i <= rf.logOffset {
		rf.mu.Unlock()
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)
	data := w.Bytes()

	rf.mu.Unlock()

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(i)
	e2.Encode(t)
	e2.Encode(s)
	snapshot := w2.Bytes()
	// DPrintf(" encoded data is %v", snapshot)
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	// DPrintf(" %d save snapshot %v", rf.me, s)

	rf.discardLog(i, t)
}

// discard log entries
// index is the last applied index in rf.log
func (rf *Raft) discardLog(index int, term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logOffset {
		return false
	}

	logIndex := index - rf.logOffset
	if logIndex < len(rf.log) {
		rf.log = rf.log[logIndex:]
	} else {
		rf.log = rf.log[len(rf.log):]
		emptylog := logEntry{}
		emptylog.Command = ""
		emptylog.Term = term
		rf.log = append(rf.log, emptylog)
	}
	rf.logOffset = index // logOffset is the last applied index, which is log 0 in the new log
	rf.lastApplied = index
	// DPrintf(" %d delete log entries before index %d, update offset to %d, lastApplied is %d, log length is %d", rf.me, index, rf.logOffset, rf.lastApplied, len(rf.log))

	// rf.mu.Unlock()
	rf.persist()
	// DPrintf(" %d save persist state on discard log: currentTerm %d, votedFor %d, log len %d, logOffset %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.logOffset)
	// rf.mu.Lock()
	return true
}


// kvserver call replay log after replay snapshot
// index is the last applied index included in the snapshot
func (rf *Raft) ReplayLog(index int) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(" %d start replay logs from %v, logoffset %v, lastApplied %v", rf.me, index, rf.logOffset, rf.lastApplied)
	for i := index; i < rf.lastApplied; i++ {
		rf.mu.Unlock()
		rf.sendApplyMsg(i + 1)
		rf.mu.Lock()
	}
	return rf.lastApplied, rf.log[rf.lastApplied - rf.logOffset].Term
}
