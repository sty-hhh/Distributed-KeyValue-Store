package shardkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

const (
	PullConfigInterval       = time.Millisecond * 100
	PullShardsInterval       = time.Millisecond * 200
	WaitCmdTimeOut           = time.Millisecond * 500 
	ReqCleanShardDataTimeOut = time.Millisecond * 500
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MsgId    	int64		// CommandArgs id (随机生成), 与 ClientId 一起判断 Command 是否重复 
	ReqId    	int64		// NotifyMsg 索引 (随机生成), 映射对应 Command 的完成结果 (返回给 RPC)
	ClientId 	int64		// 客户端 id
	Key      	string
	Value    	string
	Method   	string		// Put, Append, Get, Delete
	ConfigNum 	int
}

type NotifyMsg struct {
	Err Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config        shardmaster.Config // the latest config
	oldConfig     shardmaster.Config
	notifyCh      map[int64]chan NotifyMsg             // notify cmd done
	lastMsgIdx    [shardmaster.NShards]map[int64]int64 // clientId -> msgId map
	ownShards     map[int]bool
	data          [shardmaster.NShards]map[string]string // kv per shard
	waitShardIds  map[int]bool
	historyShards map[int]map[int]MergeShardData // configNum -> shard -> data
	mck           *shardmaster.Clerk

	stopCh         chan struct{}
	persister      *raft.Persister

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer
}

// RPC: Get
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		MsgId:    	args.MsgId,
		ReqId:    	nrand(),
		ClientId: 	args.ClientId,
		Key:      	args.Key,
		Method:   	"Get",
		ConfigNum: 	args.ConfigNum,
	}
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

// RPC: PutAppend
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		MsgId:    	args.MsgId,
		ReqId:    	nrand(),
		Key:      	args.Key,
		Value:    	args.Value,
		Method:   	args.Op,
		ClientId: 	args.ClientId,
		ConfigNum: 	args.ConfigNum,
	}
	reply.Err = kv.waitCmd(op).Err
}

// RPC: Delete
func (kv *ShardKV) Delete(args *DeleteArgs, reply *DeleteReply) {
	op := Op{
		MsgId:    	args.MsgId,
		ReqId:    	nrand(),
		Key:      	args.Key,
		Method:   	"Delete",
		ClientId: 	args.ClientId,
		ConfigNum: 	args.ConfigNum,
	}
	reply.Err = kv.waitCmd(op).Err
}

func (kv *ShardKV) removeCh(id int64) {
	kv.mu.Lock()
	delete(kv.notifyCh, id)
	kv.mu.Unlock()
}

// 将 RPC 请求的 Command 通过 Raft 写入 Logentries, 等待 Command 执行完成返回结果
func (kv *ShardKV) waitCmd(op Op) (res NotifyMsg) {
	ch := make(chan NotifyMsg, 1)
	kv.mu.Lock()
	// 这里不检查 wait shard id
	// 若是新 leader，需要想办法产生本 term 的日志
	if op.ConfigNum == 0 || op.ConfigNum < kv.config.Num {
		res.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 启动 raft
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.notifyCh[op.ReqId] = ch
	kv.mu.Unlock()
	// 设置定时器等待 Command
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

func (kv *ShardKV) isRepeated(shardId int, clientId int64, id int64) bool {
	if val, ok := kv.lastMsgIdx[shardId][clientId]; ok {
		return val == id
	}
	return false
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate{
		return
	}
	data := kv.genSnapshotData()
	kv.rf.Snapshot(logIndex, data)
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastMsgIdx)
	e.Encode(kv.waitShardIds)
	e.Encode(kv.historyShards)
	e.Encode(kv.config)
	e.Encode(kv.oldConfig)
	e.Encode(kv.ownShards)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapShotData(data []byte) {
	if data == nil || len(data) < 1 { 
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData [shardmaster.NShards]map[string]string
	var lastMsgIdx [shardmaster.NShards]map[int64]int64
	var waitShardIds map[int]bool
	var historyShards map[int]map[int]MergeShardData
	var config shardmaster.Config
	var oldConfig shardmaster.Config
	var ownShards map[int]bool
	if d.Decode(&kvData) != nil ||
		d.Decode(&lastMsgIdx) != nil ||
		d.Decode(&waitShardIds) != nil ||
		d.Decode(&historyShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil ||
		d.Decode(&ownShards) != nil {
	} else {
		kv.data = kvData
		kv.lastMsgIdx = lastMsgIdx
		kv.waitShardIds = waitShardIds
		kv.historyShards = historyShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.ownShards = ownShards
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
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
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	return kv
}