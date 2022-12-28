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
	config        shardmaster.Config // the latest config
	oldConfig     shardmaster.Config
	notifyCh      map[int64]chan NotifyMsg             // notify cmd done
	lastMsgIdx    [shardmaster.NShards]map[int64]int64 // clientId -> msgId map
	ownShards     map[int]bool
	data          [shardmaster.NShards]map[string]string // kv per shard
	waitShardIds  map[int]bool
	historyShards map[int]map[int]MergeShardData // configNum -> shard -> data
	mck           *shardmaster.Clerk

	stopCh    chan struct{}
	persister *raft.Persister

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer
}

func (kv *ShardKV) isRepeated(shardId int, clientId int64, id int64) bool {
	if val, ok := kv.lastMsgIdx[shardId][clientId]; ok {
		return val == id
	}
	return false
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	// need snapshot
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

func (kv *ShardKV) configReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.ownShards[shardId]; !ok {
		return ErrWrongGroup
	}
	if _, ok := kv.waitShardIds[shardId]; ok {
		return ErrWrongGroup
	}
	return OK
}

// the tester calls Kill() when a ShardKV instance won't be needed again. 
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}
			kv.mu.Lock()
			lastNum := kv.config.Num
			kv.mu.Unlock()
			config := kv.mck.Query(lastNum + 1)
			if config.Num == lastNum+1 {
				// 找到新的 config
				kv.mu.Lock()
				if len(kv.waitShardIds) == 0 && kv.config.Num+1 == config.Num {
					kv.mu.Unlock()
					kv.rf.Start(config.Copy())
				} else {
					kv.mu.Unlock()
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
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
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.gid)

	// You may need initialization code here.
	kv.data = [shardmaster.NShards]map[string]string{}
	for i, _ := range kv.data {
		kv.data[i] = make(map[string]string)
	}
	kv.lastMsgIdx = [shardmaster.NShards]map[int64]int64{}
	for i, _ := range kv.lastMsgIdx {
		kv.lastMsgIdx[i] = make(map[int64]int64)
	}

	kv.waitShardIds = make(map[int]bool)
	kv.historyShards = make(map[int]map[int]MergeShardData)
	config := shardmaster.Config{
		Num:    0,
		Shards: [shardmaster.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config
	kv.readSnapShotData(kv.persister.ReadSnapshot())

	kv.notifyCh = make(map[int64]chan NotifyMsg)
	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	go kv.waitApplyCh()
	go kv.pullConfig()
	go kv.pullShards()

	return kv
}
