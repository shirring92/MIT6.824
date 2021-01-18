package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Leader = 1
	nonLeader = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name string
	Key string
	Value string
	Index int
}

// type record struct {
// 	msgIndex int
// 	msgTerm int
// 	execute bool
// }

// type Snapshot struct {
// 	LastAppliedIndex int
// 	LastAppliedTerm int
// 	KVstorage map[string]string
// }

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// leaderId int
	lastAppliedIndex int
	lastAppliedTerm int
	storage map[string]string
	// commitIndex map[string]int
	// executeIndex map[int]bool
	receivedIndex map[int]raft.Record // map <cmd id, msg record>
	clientLastReq map[string]int // map <client id, last cmd id>
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("kvserver %d got Get from client", kv.me)
	// Your code here.
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	
	cmd := Op{
		Name: "Get",
		Key: args.Key,
		Value: "",
		Index: args.Index,
	}
	currentTerm, _ := kv.rf.GetState()
	// if !state {
	// 	return
	// }

	kv.clientLastReq[args.ClientId] = args.Index

	record, seen := kv.receivedIndex[args.Index]
	DPrintf("kvserver %d start new cmd %v, check status <seen %v, record %v>", kv.me, args, seen, record)
	if seen && record.Execute && kv.lastAppliedIndex >= record.MsgIndex { // already execute command
		value, ok := kv.storage[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
			// reply.LeaderId = kv.leaderId
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
			// reply.LeaderId = kv.leaderId
		}
		DPrintf("kvserver %d already execute %v from client %v", kv.me, cmd, args.Key)
		kv.mu.Unlock()
		return
	} else { 
		if !seen || record.MsgTerm != currentTerm { // new command
			index, term, leader := kv.rf.Start(cmd)
			if !leader {
				reply.Err = ErrWrongLeader
				reply.Value = ""
				// reply.LeaderId = kv.leaderId
				kv.mu.Unlock()
				return
			}
			kv.receivedIndex[args.Index] = raft.Record{
				MsgIndex: index,
				MsgTerm: term,
				Execute: false,
			}
			DPrintf("kvserver %d got new cmd %v, send to Raft as msg %d", kv.me, cmd, index)
		}
		kv.mu.Unlock()

		// still executing command
		// wait for raft to commit
		// then execute Get operation
		
		// DPrintf("kvserver %d start send Get %d <key %v> to Raft on index %d", kv.me, args.Index, args.Key, index)
		for {
			if kv.killed() {
				return
			}
			_, state := kv.rf.GetState()
			if !state {
				// DPrintf("kvserver %d is not leader anymore", kv.me)
				reply.Err = ErrWrongLeader
				reply.Value = ""
				// reply.LeaderId = kv.leaderId
				return
			}

			kv.mu.Lock()
			if kv.receivedIndex[args.Index].Execute && kv.lastAppliedIndex >= kv.receivedIndex[args.Index].MsgIndex {
				// kv.clientLastReq[args.ClientId] = args.Index
				value, ok := kv.storage[args.Key]
				if ok {
					reply.Err = OK
					reply.Value = value
					// reply.LeaderId = kv.leaderId
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
					// reply.LeaderId = kv.leaderId
				}
				DPrintf("kvserver %d finish execute %v of Raft msg %v", kv.me, cmd, kv.receivedIndex[args.Index])
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("kvserver %d got PutAppend from client", kv.me)
	// Your code here.
	kv.mu.Lock()
	// defer kv.mu.Unlock
	
	cmd := Op{
		Name: args.Op,
		Key: args.Key,
		Value: args.Value,
		Index: args.Index,
	}

	// cmdId, lastSeen := kv.clientLastReq[args.ClientId]
	// if lastSeen && args.Index == cmdId {
	// 	reply.Err = OK
	// 	DPrintf("kvserver %d got duplicate cmd %v from client %v", kv.me, cmd, args.ClientId)
	// 	kv.mu.Unlock()
	// 	return
	// }
	currentTerm, _ := kv.rf.GetState()
	// if !state {
	// 	return
	// }

	kv.clientLastReq[args.ClientId] = args.Index

	record, seen := kv.receivedIndex[args.Index]
	DPrintf("kvserver %d start new cmd %v, check status <seen %v, record %v>", kv.me, args, seen, record)
	if seen && record.Execute && kv.lastAppliedIndex >= record.MsgIndex {
		reply.Err = OK
		// reply.LeaderId = kv.leaderId
		DPrintf("kvserver %d already execute %v from client %v", kv.me, cmd, args.Key)
		kv.mu.Unlock()
		return
	} else {
		// if kvserver got cmd in term t1, replicate in its log, but before it can commit it, it become follower. then in term t2, it become leader again.
		// if the cmd is the last cmd, since raft only update commit index when last log is in current term, it will not commit the cmd if no new cmd come in.
		// so client need to send the cmd again and kvserver need to send the cmd to raft the second time.
		if !seen || record.MsgTerm != currentTerm {
			index, term, leader := kv.rf.Start(cmd)
			if !leader {
				reply.Err = ErrWrongLeader
				// reply.LeaderId = kv.leaderId
				// DPrintf("kvserver %d is not leader", kv.me)
				kv.mu.Unlock()
				return
			}
			kv.receivedIndex[args.Index] = raft.Record{
				MsgIndex: index,
				MsgTerm: term,
				Execute: false,
			}
			DPrintf("kvserver %d got new cmd %v, send to Raft as msg %d", kv.me, cmd, index)
		}
		kv.mu.Unlock()

		// wait for raft to commit
		// then excute put/append operation
			
		// DPrintf("kvserver %d start send %v %d <key %v, value %v> to Raft on index %d", kv.me, args.Op, args.Index, args.Key, args.Value, index)		
		for {
			if kv.killed() {
				return
			}
			
			_, state := kv.rf.GetState()
			if !state {
				// DPrintf("kvserver %d is not leader anymore", kv.me)
				reply.Err = ErrWrongLeader
				// reply.LeaderId = kv.leaderId
				return
			}

			kv.mu.Lock()
			if kv.receivedIndex[args.Index].Execute && kv.lastAppliedIndex >= kv.receivedIndex[args.Index].MsgIndex {
				// kv.clientLastReq[args.ClientId] = args.Index
				reply.Err = OK
				// reply.LeaderId = kv.leaderId
				DPrintf("kvserver %d finish execute %v of Raft msg %v", kv.me, cmd, kv.receivedIndex[args.Index])
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	DPrintf("--------------------------------------------check 5")
	// You may need initialization code here.
	kv.mu.Lock()

	// kv.leaderId = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastAppliedIndex = 0
	kv.lastAppliedTerm = 0
	kv.storage = make(map[string]string)
	kv.clientLastReq = make(map[string]int)
	kv.receivedIndex = make(map[int]raft.Record)
	kv.mu.Unlock()

	DPrintf("--------------------------------------------check 6")
	go kv.applyKV()
	if maxraftstate != -1 {
		kv.readSnapshot()
		go kv.checkRaftSize()
	}

	DPrintf("make kvserver %d", kv.me)

	return kv
}

func (kv *KVServer) applyKV() {
	DPrintf("kvserver %d start run applyKV", kv.me)
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			if kv.lastAppliedIndex >= msg.CommandIndex {
				DPrintf("kvserver %d already applied msg %d before, current index is %d", kv.me, msg.CommandIndex, kv.lastAppliedIndex)
			} else {
				kv.lastAppliedIndex = msg.CommandIndex
				kv.lastAppliedTerm = msg.CommandTerm
				cmd := msg.Command.(Op)
				// if kvserver is not leader, it can receive a cmd with a different index from its record
				// it's possible when it first receive the cmd, it's leader, got an index from raft, but did not commit it because of crash or partition
				// then another server take over and got the same cmd with a different index, and the leader will overwrite this server's record
				// so the cmd send back through applyCh can have an index larger than its record
				// msgIndex, ok := kv.receivedIndex[cmd.Index]
				record, ok := kv.receivedIndex[cmd.Index]
				_, isleader := kv.rf.GetState()
				DPrintf("kvserver %d got applied msg, index %d, term %d, storage status is <%v, %v>, leader status %v", kv.me, kv.lastAppliedIndex, kv.lastAppliedTerm, ok, record, isleader)
				if !ok || ok && !record.Execute {
					// if !ok || !isleader {
					kv.receivedIndex[cmd.Index] = raft.Record{
						MsgIndex: msg.CommandIndex,
						MsgTerm: msg.CommandTerm,
						Execute: true,
					}
					// }
					if cmd.Name == "Get" {
						DPrintf("kvserver %d finish %v from storage", kv.me, cmd)
					} else if cmd.Name == "Put" {
						kv.storage[cmd.Key] = cmd.Value
						DPrintf("kvserver %d finish %v to storage", kv.me, cmd)
					} else {
						val, ok := kv.storage[cmd.Key]
						if ok {
							kv.storage[cmd.Key] = val + cmd.Value
							DPrintf("kvserver %d finish %v to storage with value %v", kv.me, cmd, kv.storage[cmd.Key])
						} else {
							kv.storage[cmd.Key] = cmd.Value
							DPrintf("kvserver %d doesn't have key %v for %v, act as Put", kv.me, cmd.Key, cmd)
						}
						
					}
				}
			}
			kv.mu.Unlock()
		} else {
			// receive snapshot
			s := msg.Snapshot
			kv.replaySnapshot(s.LastAppliedIndex, s.LastAppliedTerm, s.KVstorage, s.ClientLastReq, s.ReceivedIndex)
		}
	}
}

func (kv *KVServer) replaySnapshot(i int, t int, kvs map[string]string, clr map[string]int, receiveI map[int]raft.Record) {
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	DPrintf("kvserver %d start replay snapshot: index %v, term %v, map %v", kv.me, i, t, kvs)
	if kv.lastAppliedIndex < i && kv.lastAppliedTerm <= t {
		kv.lastAppliedIndex = i
		kv.lastAppliedTerm = t
		for key, val := range(kvs) {
			kv.storage[key] = val
		}
		for key, val := range(clr) {
			kv.clientLastReq[key] = val
		}
		for key, val := range(receiveI) {
			kv.receivedIndex[key] = val
		}

	}
	kv.mu.Unlock()

	kv.rf.ReplayLog(kv.lastAppliedIndex)
}

func (kv *KVServer) readSnapshot() {
	DPrintf("kvserver %d start read snapshot from Raft", kv.me)
	ok := kv.rf.GetSnapshot()
	if !ok {
		DPrintf("kvserver %d didn't get snapshot from Raft", kv.me)
		return
	}
}

func (kv *KVServer) checkRaftSize() {
	for {
		if kv.killed() {
			return
		}

		_, state := kv.rf.GetState()
		if state {
			size := kv.rf.GetStateSize()
			DPrintf("kvserver %d got Raft state size is %d", kv.me, size)
			if kv.maxraftstate <= size {
				kv.generateSnapshot()
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) generateSnapshot() {
	kv.mu.Lock()

	snapshot := raft.Snapshot{
		LastAppliedIndex: kv.lastAppliedIndex,
		LastAppliedTerm: kv.lastAppliedTerm,
	}
	snapshot.KVstorage = make(map[string]string)
	snapshot.ClientLastReq = make(map[string]int)
	snapshot.ReceivedIndex = make(map[int]raft.Record)

	for key, val := range(kv.storage) {
		snapshot.KVstorage[key] = val
	}

	for key, val := range(kv.clientLastReq) {
		snapshot.ClientLastReq[key] = val
		snapshot.ReceivedIndex[val] = kv.receivedIndex[val]
	}

	DPrintf("kvserver %d generate snapshot %v", kv.me, snapshot)
	kv.mu.Unlock()
	
	kv.rf.LeaderSaveSnapshot(snapshot, snapshot.LastAppliedIndex, snapshot.LastAppliedTerm)
}
