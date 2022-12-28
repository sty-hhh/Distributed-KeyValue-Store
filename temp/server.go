package shardkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"sync/atomic"
	"time"
)



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

			kv.lock("pullConfig")
			lastNum := kv.config.Num
			kv.unlock("pullConfig")

			config := kv.mck.Query(lastNum + 1)
			if config.Num == lastNum+1 {
				// 找到新的 config
				kv.lock("pullConfig")
				if len(kv.waitShardIds) == 0 && kv.config.Num+1 == config.Num {
					kv.unlock("pullConfig")
					kv.rf.Start(config.Copy())
				} else {
					kv.unlock("pullConfig")
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

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
	// kv.rf.DebugLog = false
	kv.DebugLog = false

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
