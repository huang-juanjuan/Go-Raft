package main

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type HeartBeatArgs struct {
	Term     int    // 领导者的任期
	LeaderId string // 领导者的ID
}

type HeartBeatReply struct {
	Term    int  // 跟随者的当前任期
	Success bool // 心跳回应
}

func (rf *Raft) ReceiveHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	fmt.Printf("节点 %s 收到来自 %s 的心跳消息\n", rf.id, args.LeaderId)
	fmt.Println(rf.Logs)
	rf.currentTerm = args.Term
	rf.currentLeader = args.LeaderId
	rf.currentRole = Follower
	rf.lastHeartBeatTime = time.Now().UnixMilli()
	reply.Term = rf.currentTerm
	reply.Success = true

	return nil
}

func (rf *Raft) SendHeartBeat() {
	rf.mu.Lock()

	fmt.Println(rf.id + " start heartBeat")
	if rf.currentRole != Leader {
		rf.lastHeartBeatTime = 1
		rf.mu.Unlock()
		return
	}
	if len(rf.peers) == 1 {
		rf.lastHeartBeatTime = 1
		rf.currentLeader = "null"
		rf.votedFor = "null"
		rf.currentRole = Follower
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	args := &HeartBeatArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.id,
	}

	for i := 0; i < len(rf.peers); i++ {
		peer := rf.peers[i]
		if peer == "localhost:"+rf.id {
			continue
		}

		fmt.Printf("%s send heartbeat to %s\n", rf.id, peer)
		fmt.Println(rf.Logs)
		go func(peer string, index int) {
			client, err := rpc.DialHTTP("tcp", peer)
			if err != nil {
				log.Println("Dialing error:", err)
				rf.mu.Lock()
				rf.peers = append(rf.peers[:index], rf.peers[index+1:]...)
				rf.mu.Unlock()
				return
			}
			var reply HeartBeatReply
			err = client.Call("Raft.ReceiveHeartBeat", args, &reply)
			if err != nil {
				log.Println("RPC error:", err)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentRole = Follower
				rf.votedFor = "null"
				rf.currentLeader = "null"
			}
		}(peer, i)
	}
}
