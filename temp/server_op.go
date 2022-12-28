package shardkv

import (
	"fmt"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string // "Put" or "Append" Get,
	ClientId  int64
	MsgId     int64
	ReqId     int64
	ConfigNum int
}

type NotifyMsg struct {
	Err Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		MsgId:     args.MsgId,
		ReqId:     nrand(),
		Key:       args.Key,
		Op:        "Get",
		ClientId:  args.ClientId,
		ConfigNum: args.ConfigNum,
	}
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		MsgId:     args.MsgId,
		ReqId:     nrand(),
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		ConfigNum: args.ConfigNum,
	}
	reply.Err = kv.waitCmd(op).Err
}

func (kv *ShardKV) removeCh(id int64) {
	kv.lock("removeCh")
	delete(kv.notifyCh, id)
	kv.unlock("removeCh")
}

func (kv *ShardKV) waitCmd(op Op) (res NotifyMsg) {
	ch := make(chan NotifyMsg, 1)

	kv.lock("waitCmd")
	// 这里不检查 wait shard id
	// 若是新 leader，需要想办法产生本 term 的日志
	if op.ConfigNum == 0 || op.ConfigNum < kv.config.Num {
		res.Err = ErrWrongGroup
		kv.unlock("waitCmd")
		return
	}
	kv.unlock("waitCmd")

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	kv.lock("waitCmd")
	kv.notifyCh[op.ReqId] = ch
	kv.unlock("waitCmd")

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
