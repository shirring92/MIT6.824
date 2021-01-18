package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"
import "strconv"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	leaderId int
	clientId string
	// index int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.leaderId = -1
	// ck.index = 0
	ck.clientId = "client"+strconv.Itoa(int(nrand()))
	DPrintf("make client")
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	// ck.index++
	index := int(nrand())
	args := GetArgs{
		Key: key,
		Index: index,
		ClientId: ck.clientId,
	}
	reply := GetReply{}

	i := ck.leaderId
	if i == -1 {
		i = 0
	}
	ck.mu.Unlock()
	
	for {
		DPrintf("client %v send %v to server %d", key, args, i)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			DPrintf("client %v got reponse value %v for %v from server %d", key, reply.Value, args, i)
			ck.mu.Lock()
			ck.leaderId = i
			ck.mu.Unlock()
			break
		}
		if !ok && ck.leaderId == -1{
			DPrintf("client %v didn't connect with server %d on %v", key, i, args)
			i = (i + 1) % len(ck.servers)
		} else {
			DPrintf("client %v got error %v from server %d, send %v to next server", key, reply.Err, i, args)
			if reply.Err == ErrWrongLeader {
				// i = reply.leaderId
				i = (i + 1) % len(ck.servers)
				ck.mu.Lock()
				ck.leaderId = -1
				ck.mu.Unlock()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	if reply.Err == ErrNoKey {
		return ""
	} else if reply.Err == OK {
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	// ck.index++
	index := int(nrand())
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		Index: index,
		ClientId: ck.clientId,
	}
	reply := PutAppendReply{}

	i := ck.leaderId
	if i == -1 {
		i = 0
	}
	ck.mu.Unlock()

	for {
		DPrintf("client %v send %v to server %d", key, args, i)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			DPrintf("client %v got reponse for %v from server %d", key, args, i)
			ck.mu.Lock()
			ck.leaderId = i
			ck.mu.Unlock()
			return
		}
		if !ok && ck.leaderId == -1 {
			DPrintf("client %v didn't connect with server %d on %v", key, i, args)
			i = (i + 1) % len(ck.servers)
		} else {
			DPrintf("client %v got error %v from server %d, send %v RPC %d to next server", key, reply.Err, i, op, args.Index)
			if reply.Err == ErrWrongLeader {
				ck.mu.Lock()
				ck.leaderId = -1
				ck.mu.Unlock()
				i = (i + 1) % len(ck.servers)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
