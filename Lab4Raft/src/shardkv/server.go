package shardkv


import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "sync/atomic"
import "../labgob"
import "time"

const NShards = 10
const (
	DISABLE = 0
	ENABLE = 1
	INTRANSITION = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name string
	// Get/Put/Append
	Key string
	Value string
	Shard int
	Index int
	// transfer shard
	KVmap map[string]string
	Dupmap map[int]bool
	// Config
	Config shardmaster.Config
	CfgNum int
	S raft.Shard
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32
	mck *shardmaster.Clerk
	lastCfg shardmaster.Config
	lastAppliedIndex int
	lastAppliedTerm int
	lastTransCfg int
	shards [NShards]raft.Shard
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	if !kv.shards[args.Shard].Status {
		reply.Err = ErrWrongGroup
		reply.Value = ""
		DPrintf("shardkv (%d: %d) got %v from client, reply err on last cfg %v, shards %v", kv.gid, kv.me, args, kv.lastCfg, kv.shards)
		kv.mu.Unlock()
		return
	}

	cmd := Op{
		Name: "Get",
		Key: args.Key,
		Value: "",
		Shard: args.Shard,
		Index: args.Index,
	}

	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		kv.mu.Unlock()
		return
	}
	DPrintf("shardkv (%d: %d) send cmd %v to raft, got index %d", kv.gid, kv.me, cmd, index)
	kv.mu.Unlock()

	for {
		if kv.killed() {
			reply.Err = ErrWrongLeader
			reply.Value = ""
			return
		}
		_, state := kv.rf.GetState()
		if !state {
			reply.Err = ErrWrongLeader
			reply.Value = ""
			return
		}

		kv.mu.Lock()
		if kv.lastAppliedIndex >= index {
			if !kv.shards[args.Shard].Status {
				reply.Err = ErrWrongGroup
				reply.Value = ""
			} else {
				v, ok := kv.shards[args.Shard].KVstorage[args.Key]
				if !ok {
					reply.Err = ErrNoKey
					reply.Value = ""
				} else {
					reply.Err = OK
					reply.Value = v
				}
			}
			DPrintf("shardkv (%d: %d) reply with %v for Get", kv.gid, kv.me, reply)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	if !kv.shards[args.Shard].Status {
		reply.Err = ErrWrongGroup
		DPrintf("shardkv (%d: %d) got %v from client, reply err on last cfg %v, shards %v", kv.gid, kv.me, args, kv.lastCfg, kv.shards)
		kv.mu.Unlock()
		return
	}

	cmd := Op{
		Name: args.Op,
		Key: args.Key,
		Value: args.Value,
		Shard: args.Shard,
		Index: args.Index,
	}

	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("shardkv (%d: %d) send cmd %v to raft, got index %d", kv.gid, kv.me, cmd, index)
	kv.mu.Unlock()

	for {
		if kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}
		_, state := kv.rf.GetState()
		if !state {
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		if kv.lastAppliedIndex >= index {
			if !kv.shards[args.Shard].Status {
				reply.Err = ErrWrongGroup
			} else {
				if args.Op == "Put" {
					reply.Err = OK
				} else if args.Op == "Append" {
					_, ok := kv.shards[args.Shard].KVstorage[args.Key]
					if !ok {
						reply.Err = ErrNoKey
					} else {
						reply.Err = OK
					}
				}
			}
			DPrintf("shardkv (%d: %d) reply with %v for PutAppend", kv.gid, kv.me, reply)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()

	cmd := Op{
		Name: "TransferShard",
		Shard: args.Shard,
		CfgNum: args.CfgNum,
	}
	cmd.KVmap = make(map[string]string)
	for k := range args.KVmap {
		cmd.KVmap[k] = args.KVmap[k]
	}
	cmd.Dupmap = make(map[int]bool)
	for k, v := range args.Dupmap {
		cmd.Dupmap[k] = v
	}

	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("shardkv (%d: %d) send cmd %v to raft, got index %d", kv.gid, kv.me, cmd, index)
	kv.mu.Unlock()

	for {
		if kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}
		_, state := kv.rf.GetState()
		if !state {
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		if kv.lastAppliedIndex >= index {
			reply.Err = OK
			DPrintf("shardkv (%d: %d) reply with %v for TransferShard %d", kv.gid, kv.me, reply, args.Shard)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastCfg.Groups = make(map[int][]string)

	for i := 0; i < NShards; i++ {
		kv.shards[i].Num = i
		kv.shards[i].KVstorage = make(map[string]string)
		kv.shards[i].Dupmap = make(map[int]bool)
		go kv.checkShard(i)
	}
	go kv.appliedLog()
	go kv.getLastConfig()
	if maxraftstate != -1 {
		kv.readSnapshot()
		go kv.checkRaftSize()
	} else {
		kv.rf.ReplayLog(0)
	}
	DPrintf("shardkv (%d: %d) start", kv.gid, kv.me)
	return kv
}

func (kv *ShardKV) checkShard(id int) {
	for {
		if kv.killed() {
			return
		}
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("shardkv (%d: %d) lastCfg %v, shard %d lastcfg is %d", kv.gid, kv.me, kv.lastCfg, id, kv.shards[id].LastCfg)
			if kv.lastCfg.Num > kv.shards[id].LastCfg {
				if kv.lastCfg.Num == 1 && kv.lastCfg.Shards[id] == kv.gid {
					DPrintf("shardkv (%d: %d) enable shard %d on cfg %d", kv.gid, kv.me, id, kv.lastCfg.Num)
					kv.shards[id].Gid = kv.gid
					kv.shards[id].LastCfg = 1
					kv.shards[id].Status = true
					cmd := Op{
						Name: "UpdateShard",
					}
					cmd.S = raft.Shard{
						Num: kv.shards[id].Num,
						Gid: kv.shards[id].Gid,
						LastCfg: kv.shards[id].LastCfg,
						Status: kv.shards[id].Status,
					}
					_, _, _ = kv.rf.Start(cmd)
				} else if kv.shards[id].Status && kv.lastCfg.Shards[id] != kv.gid {
					// current shard belongs to this group and need to transfer to other group
					kv.shards[id].Gid = kv.lastCfg.Shards[id]
					kv.shards[id].Status = false
					DPrintf("shardkv (%d: %d) see config change to %d for shard %d on cfg %d", kv.gid, kv.me, kv.shards[id].Gid, id, kv.lastCfg.Num)

					args := TransferShardArgs{}
					args.Shard = id
					args.KVmap = make(map[string]string)
					for k, v := range(kv.shards[id].KVstorage) {
						args.KVmap[k] = v
					}
					args.Dupmap = make(map[int]bool)
					for k, v := range(kv.shards[id].Dupmap) {
						args.Dupmap[k] = v
					}
					args.CfgNum = kv.lastCfg.Num
					reply := TransferShardReply{}			

					gid := kv.shards[id].Gid
					servers := kv.lastCfg.Groups[gid]
					kv.mu.Unlock()
					ret := kv.sendTransferShard(gid, servers, &args, &reply)
					kv.mu.Lock()
					if ret {
						kv.shards[id].LastCfg = args.CfgNum
						kv.shards[id].KVstorage = make(map[string]string)
						kv.shards[id].Dupmap = make(map[int]bool)
						cmd := Op{
							Name: "UpdateShard",
						}
						cmd.S = raft.Shard{
							Num: kv.shards[id].Num,
							Gid: kv.shards[id].Gid,
							LastCfg: kv.shards[id].LastCfg,
							Status: kv.shards[id].Status,
						}
						_, _, _ = kv.rf.Start(cmd)
					} else {
						kv.shards[id].Gid = kv.gid
						kv.shards[id].Status = true
					}
				} else if kv.shards[id].Status && kv.lastCfg.Shards[id] == kv.gid {
					// current shard belongs to this group and unchanged
					DPrintf("shardkv (%d: %d) has shard %d unchanged on cfg %d", kv.gid, kv.me, id, kv.lastCfg.Num)
					kv.shards[id].LastCfg = kv.lastCfg.Num
					cmd := Op{
						Name: "UpdateShard",
					}
					cmd.S = raft.Shard{
						Num: kv.shards[id].Num,
						Gid: kv.shards[id].Gid,
						LastCfg: kv.shards[id].LastCfg,
						Status: kv.shards[id].Status,
					}
					_, _, _ = kv.rf.Start(cmd)
				} else if !kv.shards[id].Status {
					// current shard is not belong to this group
					DPrintf("shardkv (%d: %d) do not have shard %d on cfg %d", kv.gid, kv.me, id, kv.lastCfg.Num)
				}
			}
			DPrintf("shardkv (%d: %d) lastCfg %v, shard %d lastcfg is %d", kv.gid, kv.me, kv.lastCfg, id, kv.shards[id].LastCfg)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendTransferShard(gid int, servers []string, args *TransferShardArgs, reply *TransferShardReply) bool {
		// DPrintf("shardkv (%d: %d) should transfer shard %d to group %d", kv.gid, kv.me, i, cfg.Shards[i])
	for {
		for i := 0; i < len(servers); i++ {
			DPrintf("shardkv (%d: %d) send TransferShard to (%d: %d) on %v", kv.gid, kv.me, gid, i, args)
			srv := kv.make_end(servers[i])
			ok := srv.Call("ShardKV.TransferShard", args, reply)
			if ok && reply.Err == OK {
				DPrintf("shardkv (%d: %d) send TransferShard to (%d: %d) on %v success", kv.gid, kv.me, gid, i, args)
				return true
			}
			if ok && reply.Err == ErrWrongGroup {
				DPrintf("shardkv (%d: %d) send TransferShard to (%d: %d) on %v fail. wrong group", kv.gid, kv.me, gid, i, args)
				return false
			}
			// ok && wrongleader || !ok
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (kv *ShardKV) getLastConfig() {
	for {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if isLeader {
			cfg := kv.mck.Query(-1)
			if cfg.Num > kv.lastCfg.Num {
				cmd := Op {
					Name: "Config",
					Config: cfg,
				}
				_, _, _ = kv.rf.Start(cmd)
				DPrintf("shardkv (%d: %d) send config %v to Raft", kv.gid, kv.me, cfg)
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) appliedLog() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			if kv.lastAppliedIndex >= msg.CommandIndex {
				DPrintf("shardkv (%d: %d) already applied msg %d before, current index is %d", kv.gid, kv.me, msg.CommandIndex, kv.lastAppliedIndex)
			} else {
				kv.lastAppliedIndex = msg.CommandIndex
				kv.lastAppliedTerm = msg.CommandTerm
				cmd := msg.Command.(Op)
				DPrintf("shardkv (%d: %d) apply msg %d: %v from raft", kv.gid, kv.me, kv.lastAppliedIndex, cmd)
				if cmd.Name == "Put" {
					if kv.shards[cmd.Shard].Status {
						if status, ok := kv.shards[cmd.Shard].Dupmap[cmd.Index]; ok && status {
							DPrintf("shardkv (%d: %d) already applied %v", kv.gid, kv.me, cmd)
						} else {
							kv.shards[cmd.Shard].KVstorage[cmd.Key] = cmd.Value
							kv.shards[cmd.Shard].Dupmap[cmd.Index] = true
						}
					}
					DPrintf("shardkv (%d: %d) after apply put on shard %v", kv.gid, kv.me, kv.shards[cmd.Shard])
				} else if cmd.Name == "Append" {
					if kv.shards[cmd.Shard].Status {
						if status, ok := kv.shards[cmd.Shard].Dupmap[cmd.Index]; ok && status {
							DPrintf("shardkv (%d: %d) already applied %v", kv.gid, kv.me, cmd)
						} else {
							// v, ok := kv.storage[cmd.Key]
							v, ok := kv.shards[cmd.Shard].KVstorage[cmd.Key]
							if !ok {
								kv.shards[cmd.Shard].KVstorage[cmd.Key] = cmd.Value
							} else {
								kv.shards[cmd.Shard].KVstorage[cmd.Key] = v + cmd.Value
							}
							kv.shards[cmd.Shard].Dupmap[cmd.Index] = true;
						}
					}
					DPrintf("shardkv (%d: %d) after apply append on shard %v", kv.gid, kv.me, kv.shards[cmd.Shard])
				} else if cmd.Name == "TransferShard" { // received shards from other groups
					// if kv.enabledShard[cmd.Shard] != ENABLE {
					if cmd.CfgNum > kv.shards[cmd.Shard].LastCfg {
						kv.shards[cmd.Shard].Gid = kv.gid
						kv.shards[cmd.Shard].LastCfg = cmd.CfgNum
						kv.shards[cmd.Shard].KVstorage = make(map[string]string)
						for k, v := range cmd.KVmap {
							kv.shards[cmd.Shard].KVstorage[k] = v
						}
						kv.shards[cmd.Shard].Status = true
						kv.shards[cmd.Shard].Dupmap = make(map[int]bool)
						for k, v := range cmd.Dupmap {
							kv.shards[cmd.Shard].Dupmap[k] = v
						}
						DPrintf("shardkv (%d: %d) receive transfer shard %v", kv.gid, kv.me, kv.shards[cmd.Shard])
					}
					DPrintf("shardkv (%d: %d) shards %v", kv.gid, kv.me, kv.shards)
				} else if cmd.Name == "Config" {
					kv.lastCfg = cmd.Config
					DPrintf("shardkv (%d: %d) update config %v", kv.gid, kv.me, kv.lastCfg)
				} else if cmd.Name == "UpdateShard" {
					if _, isLeader := kv.rf.GetState(); !isLeader {
						id := cmd.S.Num
						kv.shards[id].Num = id
						kv.shards[id].Gid = cmd.S.Gid
						kv.shards[id].LastCfg = cmd.S.LastCfg
						kv.shards[id].Status = cmd.S.Status
						kv.shards[id].KVstorage = make(map[string]string)
						for k, v := range cmd.S.KVstorage {
							kv.shards[id].KVstorage[k] = v
						}
						kv.shards[id].Dupmap = make(map[int]bool)
						for k, v := range cmd.S.Dupmap {
							kv.shards[id].Dupmap[k] = v
						}
						DPrintf("shardkv (%d: %d) update shard %v", kv.gid, kv.me, kv.shards[id])
					}
				}
			}
			kv.mu.Unlock()
		} else { // snapshot
			DPrintf("shardkv (%d: %d) apply snapshot %v from raft", kv.gid, kv.me, msg.Snapshot)
			s := msg.Snapshot
			kv.replaySnapshot(s.LastAppliedIndex, s.LastAppliedTerm, s.Shards)
		}
	}
}

// -----------------------Snapshot-------------------------

func (kv *ShardKV) replaySnapshot(i int, t int, shards [NShards]raft.Shard) {
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	DPrintf("shardkv (%d: %d) start replay snapshot: index %v, term %v, shards %v", kv.gid, kv.me, i, t, shards)
	if kv.lastAppliedIndex < i && kv.lastAppliedTerm <= t {
		kv.lastAppliedIndex = i
		kv.lastAppliedTerm = t

		for i := 0; i < NShards; i++ {
			kv.shards[i].Num = shards[i].Num
			kv.shards[i].Gid = shards[i].Gid
			kv.shards[i].LastCfg = shards[i].LastCfg
			kv.shards[i].Status = shards[i].Status

			kv.shards[i].KVstorage = make(map[string]string)
			for k, v := range shards[i].KVstorage {
				kv.shards[i].KVstorage[k] = v
			}
			kv.shards[i].Dupmap = make(map[int]bool)
			for k, v := range(shards[i].Dupmap) {
				kv.shards[i].Dupmap[k] = v
			}
		}
	}
	DPrintf("shardkv (%d: %d): after replay snapshot shards %v", kv.gid, kv.me, kv.shards)
	kv.mu.Unlock()

	kv.rf.ReplayLog(kv.lastAppliedIndex)
}

func (kv *ShardKV) readSnapshot() {
	DPrintf("shardkv (%d: %d) start read snapshot from Raft", kv.gid, kv.me)
	ok := kv.rf.GetSnapshot()
	if !ok {
		DPrintf("shardkv (%d: %d) didn't get snapshot from Raft", kv.gid, kv.me)
		return
	}
}

func (kv *ShardKV) checkRaftSize() {
	for {
		if kv.killed() {
			return
		}

		_, state := kv.rf.GetState()
		if state {
			size := kv.rf.GetStateSize()
			DPrintf("shardkv (%d: %d) got Raft state size is %d", kv.gid, kv.me, size)
			if kv.maxraftstate <= size {
				kv.generateSnapshot()
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) generateSnapshot() {
	kv.mu.Lock()

	snapshot := raft.Snapshot{
		LastAppliedIndex: kv.lastAppliedIndex,
		LastAppliedTerm: kv.lastAppliedTerm,
	}
	var shards [NShards]raft.Shard
	for i := 0; i < NShards; i++ {
		shards[i] = raft.Shard{
			Num: kv.shards[i].Num,
			Gid: kv.shards[i].Gid,
			LastCfg: kv.shards[i].LastCfg,
			Status: kv.shards[i].Status,
		}
		shards[i].KVstorage = make(map[string]string)
		for k, v := range(kv.shards[i].KVstorage) {
			shards[i].KVstorage[k] = v
		}
		shards[i].Dupmap = make(map[int]bool)
		for k, v := range(kv.shards[i].Dupmap) {
			shards[i].Dupmap[k] = v
		}
	}
	snapshot.Shards = shards

	DPrintf("shardkv (%d: %d) generate snapshot %v", kv.gid, kv.me, snapshot)
	kv.mu.Unlock()
	
	kv.rf.LeaderSaveSnapshot(snapshot, snapshot.LastAppliedIndex, snapshot.LastAppliedTerm)
}
