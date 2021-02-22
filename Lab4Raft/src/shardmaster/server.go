package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "hash/maphash"
import "time"
import "strconv"
import "sort"

const NLoad = 360

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	shards [NShards]int
	servers map[int][]string
	inProgress map[int][2]int
	hashring map[int]int

	cfgNum int
	lastAppliedCfg int
	lastAppliedIndex int

	h maphash.Hash
	// use 3 different hashing function for virtual nodes
	h1 maphash.Hash
	h2 maphash.Hash
	h3 maphash.Hash
}


type Op struct {
	// Your data here.
	// Config Config
	Config string
	//Join
	Servers map[int][]string
	//Leave
	GIDs []int
	//Move
	Shard int
	GID int
	//Query
	Num int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isleader := sm.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		return
	}

	sm.mu.Lock()
	// cfg := Config{}
	// sm.cfgNum++
	// cfg.Num = sm.cfgNum
	// cfg.Groups = make(map[int][]string)
	// for key := range sm.servers {
	// 	cfg.Groups[key] = sm.servers[key]
	// }
	// for s := range args.Servers {
	// 	cfg.Groups[s] = args.Servers[s]
	// }

	// i := 0
	// for s := range cfg.Groups {
	// 	cfg.Shards[i] = s
	// 	i++
	// 	if i >= NShards {
	// 		break
	// 	}
	// }
	// for i < NShards {
	// 	cfg.Shards[i] = cfg.Shards[i - len(cfg.Groups)]
	// 	i++
	// }

	// cmd := Op{
	// 	Config: cfg,
	// }
	// index, term, status := sm.rf.Start(cmd)
	// DPrintf("server %d send Join %v cfg %v to Raft as cmd %d", sm.me, args.Servers, cfg, index)
	// if !status {
	// 	reply.WrongLeader = true
	// 	reply.Err = "wrong leader"
	// 	sm.mu.Unlock()
	// 	return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	// }
	cmd := Op{}
	cmd.Config = "Join"
	cmd.Servers = make(map[int][]string)
	for s := range args.Servers {
		cmd.Servers[s] = args.Servers[s]
	}
	index, _, status := sm.rf.Start(cmd)
	DPrintf("shardmaster %d send Join %v to Raft as cmd %d", sm.me, args, index)
	if !status {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		sm.mu.Unlock()
		return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	}
	sm.mu.Unlock()
	// once receive applied cfg from raft
	// add cfg to sm.configs
	for {
		sm.mu.Lock()
		if sm.lastAppliedIndex >= index {
			// for i := 0; i < NShards; i++ {
			// 	sm.shards[i] = cfg.Shards[i]
			// }
			// for s := range args.Servers {
			// 	sm.servers[s] = args.Servers[s]
			// }
 			sm.mu.Unlock()
			break
		}
		sm.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
	reply.WrongLeader = false
	reply.Err = ""
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isleader := sm.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		return
	}

	sm.mu.Lock()
	// cfg := Config{}
	// sm.cfgNum++
	// cfg.Num = sm.cfgNum
	// cfg.Groups = make(map[int][]string)
	// for key := range sm.servers {
	// 	cfg.Groups[key] = sm.servers[key]
	// }
	// for i := 0; i < len(args.GIDs); i++ {
	// 	id := args.GIDs[i]
	// 	_, ok := cfg.Groups[id]
	// 	if ok {
	// 		delete(cfg.Groups, id)
	// 	}
	// }

	// i := 0
	// for s := range cfg.Groups {
	// 	cfg.Shards[i] = s
	// 	i++
	// 	if i >= NShards {
	// 		break
	// 	}
	// }
	// for i < NShards {
	// 	cfg.Shards[i] = cfg.Shards[i - len(cfg.Groups)]
	// 	i++
	// }

	// cmd := Op{
	// 	Config: cfg,
	// }
	// index, term, status := sm.rf.Start(cmd)
	// DPrintf("server %d send Leave %v cfg %v to Raft as cmd %d", sm.me, args.GIDs, cfg, index)
	// if !status {
	// 	reply.WrongLeader = true
	// 	reply.Err = "wrong leader"
	// 	sm.mu.Unlock()
	// 	return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	// }

	cmd := Op{}
	cmd.Config = "Leave"
	cmd.GIDs = args.GIDs
	// for i := 0; i < len(args.GIDs); i++ {
	// 	cmd.GIDs[i] = args.GIDs[i]
	// }
	index, _, status := sm.rf.Start(cmd)
	DPrintf("shardmaster %d send Leave %v to Raft as cmd %d", sm.me, args, index)
	if !status {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		sm.mu.Unlock()
		return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	}
	sm.mu.Unlock()

	// once receive applied cfg2 from raft
	// add cfg2 to sm.configs
	for {
		sm.mu.Lock()
		if sm.lastAppliedIndex >= index {
			// for i := 0; i < NShards; i++ {
			// 	sm.shards[i] = cfg.Shards[i]
			// }
			// for i := 0; i < len(args.GIDs); i++ {
			// 	id := args.GIDs[i]
			// 	_, ok := sm.servers[id]
			// 	if ok {
			// 		delete(sm.servers, id)
			// 	}
			// }
			sm.mu.Unlock()
			break
		}
		sm.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
	reply.WrongLeader = false
	reply.Err = ""
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isleader := sm.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		return
	}

	sm.mu.Lock()
	// cfg := Config{}
	// sm.cfgNum++
	// cfg.Num = sm.cfgNum
	// for i := 0; i < NShards; i++ {
	// 	cfg.Shards[i] = sm.shards[i]
	// }
	// cfg.Shards[args.Shard] = args.GID
	// cfg.Groups = make(map[int][]string)
	// for key := range sm.servers {
	// 	cfg.Groups[key] = sm.servers[key]
	// }

	// cmd := Op{
	// 	Config: cfg,
	// }
	// index, term, status := sm.rf.Start(cmd)
	// DPrintf("server %d send Move %v: %v cfg %v to Raft as cmd %d", sm.me, args.Shard, args.GID, cfg, index)
	// if !status {
	// 	reply.WrongLeader = true
	// 	reply.Err = "wrong leader"
	// 	sm.mu.Unlock()
	// 	return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	// }
	cmd := Op{}
	cmd.Config = "Move"
	cmd.Shard = args.Shard
	cmd.GID = args.GID
	index, _, status := sm.rf.Start(cmd)
	DPrintf("shardmaster %d send Move %v to Raft as cmd %d", sm.me, args, index)
	if !status {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		sm.mu.Unlock()
		return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	}
	sm.mu.Unlock()

	// once receive applied cfg2 from raft
	// add cfg2 to sm.configs
	// move shard to gid
	for {
		sm.mu.Lock()
		if sm.lastAppliedIndex >= index {
			// sm.shards[args.Shard] = args.GID
			sm.mu.Unlock()
			break
		}
		sm.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
	reply.WrongLeader = false
	reply.Err = ""
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isleader := sm.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		return
	}

	sm.mu.Lock()
	// sm.cfgNum++
	// cfg := Config{}
	// cfg.Num = sm.cfgNum
	// for i := 0; i < NShards; i++ {
	// 	cfg.Shards[i] = sm.shards[i]
	// }
	// cfg.Groups = make(map[int][]string)
	// for key := range sm.servers {
	// 	cfg.Groups[key] = sm.servers[key]
	// }

	// cmd := Op{
	// 	Config: cfg,
	// }
	// index, term, status := sm.rf.Start(cmd)
	// DPrintf("server %d send Query %v cfg %v to Raft as cmd %d", sm.me, args.Num, cfg, index)
	// if !status {
	// 	reply.WrongLeader = true
	// 	reply.Err = "wrong leader"
	// 	reply.Config = Config{}
	// 	sm.mu.Unlock()
	// 	return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	// }
	cmd := Op{}
	cmd.Config = "Query"
	cmd.Num = args.Num
	index, _, status := sm.rf.Start(cmd)
	DPrintf("shardmaster %d send Query %v to Raft as cmd %d", sm.me, args, index)
	if !status {
		reply.WrongLeader = true
		reply.Err = "wrong leader"
		reply.Config = Config{}
		sm.mu.Unlock()
		return
	// } else {
	// 	cmdinfo := [2]int{index, term}
	// 	sm.inProgress[cfg.Num] = cmdinfo
	}
	sm.mu.Unlock()

	for {
		sm.mu.Lock()
		if sm.lastAppliedIndex >= index {
			DPrintf("shardmaster %d got Query %d, config length is %d, last config is %v", sm.me, args.Num, len(sm.configs), sm.configs[sm.lastAppliedCfg])
			if args.Num == -1 || args.Num > sm.lastAppliedCfg {
				reply.WrongLeader = false
				reply.Err = "config number out of range"
				reply.Config = sm.configs[sm.lastAppliedCfg]
			} else {
				reply.WrongLeader = false
				reply.Err = ""
				reply.Config = sm.configs[args.Num]
			}
			sm.mu.Unlock()
			break
		}
		sm.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.inProgress = make(map[int][2]int)
	sm.servers = make(map[int][]string)
	sm.hashring = make(map[int]int)
	sm.lastAppliedCfg = 0

	go sm.configApply()
	DPrintf("shardmaster %d start", sm.me)

	return sm
}

func (sm *ShardMaster) configApply() {
	for msg := range sm.applyCh {
		if msg.CommandValid {
			sm.mu.Lock()
			if sm.lastAppliedIndex >= msg.CommandIndex {

			} else {
				cmd := msg.Command.(Op)
				sm.lastAppliedIndex = msg.CommandIndex
				if cmd.Config == "Join" {
					lastCfg := len(sm.configs) - 1
					cfg := sm.configs[lastCfg]
					cfg2 := Config{}
					cfg2.Num = lastCfg + 1
					cfg2.Groups = make(map[int][]string)
					for s := range cfg.Groups {
						cfg2.Groups[s] = cfg.Groups[s]
					}
					for s := range cmd.Servers {
						cfg2.Groups[s] = cmd.Servers[s]
					}

					var ser []int
					for s := range cfg2.Groups {
						ser = append(ser, s)
					}
					sort.Ints(ser)

					i := 0
					for s := range ser {
						cfg2.Shards[i] = ser[s]
						i++
						if (i >= NShards) {
							break
						}
					}
					for i < NShards {
						cfg2.Shards[i] = cfg2.Shards[i - len(cfg2.Groups)]
						i++
					}
					sm.configs = append(sm.configs, cfg2)
					sm.lastAppliedCfg = cfg2.Num
					DPrintf("shardmaster %d update lastAppliedCfg to %d on Join， config is %v", sm.me, sm.lastAppliedCfg, sm.configs[sm.lastAppliedCfg])
				} else if cmd.Config == "Leave" {
					lastCfg := len(sm.configs) - 1
					cfg := sm.configs[lastCfg]
					cfg2 := Config{}
					cfg2.Num = lastCfg + 1
					cfg2.Groups = make(map[int][]string)
					for s := range cfg.Groups {
						cfg2.Groups[s] = cfg.Groups[s]
					}
					for i := range cmd.GIDs {
						_, ok := cfg2.Groups[cmd.GIDs[i]]
						if ok {
							delete(cfg2.Groups, cmd.GIDs[i])
						}
					}

					var ser []int
					for s := range cfg2.Groups {
						ser = append(ser, s)
					}
					sort.Ints(ser)

					i := 0
					for s := range ser {
						cfg2.Shards[i] = ser[s]
						i++
						if (i >= NShards) {
							break
						}
					}
					for i < NShards {
						cfg2.Shards[i] = cfg2.Shards[i - len(cfg2.Groups)]
						i++
					}
					sm.configs = append(sm.configs, cfg2)
					sm.lastAppliedCfg = cfg2.Num
					DPrintf("shardmaster %d update lastAppliedCfg to %d on Leave， config is %v", sm.me, sm.lastAppliedCfg, sm.configs[sm.lastAppliedCfg])
				} else if cmd.Config == "Move" {
					lastCfg := len(sm.configs) - 1
					cfg := sm.configs[lastCfg]
					cfg2 := Config{}
					cfg2.Num = lastCfg + 1
					cfg2.Groups = make(map[int][]string)
					for s := range cfg.Groups {
						cfg2.Groups[s] = cfg.Groups[s]
					}
					for i := 0; i < NShards; i++ {
						cfg2.Shards[i] = cfg.Shards[i]
					}
					cfg2.Shards[cmd.Shard] = cmd.GID
					sm.configs = append(sm.configs, cfg2)
					sm.lastAppliedCfg = cfg2.Num
					DPrintf("shardmaster %d update lastAppliedCfg to %d on Move， config is %v", sm.me, sm.lastAppliedCfg, sm.configs[sm.lastAppliedCfg])
				} else {
					lastCfg := len(sm.configs) - 1;
					// cfg := sm.configs[lastCfg]
					// cfg2 := Config{}
					// cfg2.Num = lastCfg + 1
					// cfg2.Groups = make(map[int][]string)
					// for s := range cfg.Groups {
					// 	cfg2.Groups[s] = cfg.Groups[s]
					// }
					// for i := 0; i < NShards; i++ {
					// 	cfg2.Shards[i] = cfg.Shards[i]
					// }
					// sm.configs = append(sm.configs, cfg2)
					// sm.lastAppliedCfg = cfg2.Num
					sm.lastAppliedCfg = lastCfg
					DPrintf("shardmaster %d update lastAppliedCfg to %d on Query， config is %v", sm.me, sm.lastAppliedCfg, sm.configs[sm.lastAppliedCfg])
				}
			}
			sm.mu.Unlock()
		}
				
		// DPrintf("server %d update lastAppliedCfg to %d， config is %v", sm.me, sm.lastAppliedCfg, sm.configs[sm.lastAppliedCfg])
	}
}

// consistant hashing
func (sm *ShardMaster) consistantHashing(h maphash.Hash, v int) int {
	h.SetSeed(h.Seed())
	h.Reset()
	bs := []byte(strconv.Itoa(v))
	h.Write(bs)
	hashValue := int(h.Sum64())
	return hashValue
}

func (sm *ShardMaster) getVirtualNode(v int) int {
	for j := 0; j < NLoad; j++ {
		sv := (v + j) % NLoad
		_, ok := sm.hashring[sv]
		if ok {
			return sm.hashring[sv]
		}
	}
	return -1
}