package kvraft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const WaitCmdTimeOut = time.Millisecond * 500	

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MsgId    msgId
	ReqId    int64
	ClientId int64
	Key      string
	Value    string
	Method   string
}

type NotifyMsg struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	msgNotify   map[int64]chan NotifyMsg
	lastApplies map[int64]msgId // last apply put/append msg
	data        map[string]string
	persister      *raft.Persister
}

func (kv *KVServer) dataGet(key string) (err Err, val string) {
	if v, ok := kv.data[key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		MsgId:    args.MsgId,
		ReqId:    nrand(),
		Key:      args.Key,
		Method:   "Get",
		ClientId: args.ClientId,
	}
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

// RPC: client 调用 server
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		MsgId:    args.MsgId,
		ReqId:    nrand(),
		Key:      args.Key,
		Value:    args.Value,
		Method:   args.Op,
		ClientId: args.ClientId,
	}
	reply.Err = kv.waitCmd(op).Err
}

func (kv *KVServer) removeCh(id int64) {
	kv.mu.Lock()
	delete(kv.msgNotify, id)
	kv.mu.Unlock()
}

func (kv *KVServer) waitCmd(op Op) (res NotifyMsg) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	kv.msgNotify[op.ReqId] = ch
	kv.mu.Unlock()
	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.removeCh(op.ReqId)
		return
	case <-t.C:
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeOut
		return
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isRepeated(clientId int64, id msgId) bool {
	if val, ok := kv.lastApplies[clientId]; ok {
		return val == id
	}
	return false
}

func (kv *KVServer) waitApplyCh() {
	for !kv.killed() {
		msg := <-kv.applyCh		// 等待通道
		if !msg.CommandValid {
			kv.mu.Lock()
			kv.readPersist(kv.persister.ReadSnapshot())
			kv.mu.Unlock()
			continue
		}
		msgIdx := msg.CommandIndex
		op := msg.Command.(Op)
		kv.mu.Lock()
		isRepeated := kv.isRepeated(op.ClientId, op.MsgId)
		switch op.Method {
		case "Put":
			if !isRepeated {
				kv.data[op.Key] = op.Value
				kv.lastApplies[op.ClientId] = op.MsgId
			}
		case "Append":
			if !isRepeated {
				_, v := kv.dataGet(op.Key)
				kv.data[op.Key] = v + op.Value
				kv.lastApplies[op.ClientId] = op.MsgId
			}
		case "Get":
		default:
			panic(fmt.Sprintf("unknown method: %s", op.Method))
		}
		kv.saveSnapshot(msgIdx)
		if ch, ok := kv.msgNotify[op.ReqId]; ok {
			_, v := kv.dataGet(op.Key)
			ch <- NotifyMsg{
				Err:   OK,
				Value: v,
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	data := kv.genSnapshotData()
	kv.rf.Snapshot(logIndex, data)
}

func (kv *KVServer) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err)
	} else if err := e.Encode(kv.lastApplies); err != nil {
		panic(err)
	}
	data := w.Bytes()
	return data
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var lastApplies map[int64]msgId
	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil {
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
	}
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
	kv.persister = persister

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastApplies = make(map[int64]msgId)
	kv.readPersist(kv.persister.ReadSnapshot())

	// start server
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.msgNotify = make(map[int64]chan NotifyMsg)
	go kv.waitApplyCh()

	return kv
}