package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	NodeId       string // 候选人的ID
	Term         int    // 发起请求节点的 Term
	LastLogIndex int    // 最新的日志索引
	LastLogTerm  int    // 最新日志的 Term
}

type RequestVoteReply struct {
	NodeId      string // Follower 的 ID
	Term        int    // 响应节点的 Term
	VoteGranted bool   // 是否同意投票
}

// 修改节点为候选人状态
func (rf *Raft) becomeCandidate() bool {
	//休眠随机时间后，再开始成为候选人
	r := rand.Int63n(3000) + 1000
	time.Sleep(time.Duration(r) * time.Millisecond)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole == Follower && rf.currentLeader == "null" && rf.votedFor == "null" {
		rf.currentRole = Candidate
		rf.votedFor = rf.id
		rf.currentTerm += 1
		rf.voteReceived = append(rf.voteReceived, "localhost:"+rf.id)
		fmt.Println("本节点已变更为候选人状态")
		return true
	} else {
		return false
	}
}

func (rf *Raft) startElection(args *RequestVoteArgs) {
	// 开始选举的时候才开始选举定时
	rf.resetElectionTimer()
	fmt.Print("开始选举Leader\n")

	var wg sync.WaitGroup
	var mu sync.Mutex

	// 决定选举是否中断
	cancelled := false

	for i := 0; i < len(rf.peers); i++ {
		peer := rf.peers[i]
		if peer == "localhost:"+rf.id {
			continue
		}
		wg.Add(1)
		go func(peer string, index int) {
			defer wg.Done()

			if rf.currentRole != Candidate {
				return
			}
			fmt.Printf("节点 %s 向 %s 发起投票请求\n", rf.id, peer)

			client, err := rpc.DialHTTP("tcp", peer)
			if err != nil {
				log.Println("Dialing error: ", err)
				rf.mu.Lock()
				rf.peers = append(rf.peers[:index], rf.peers[index+1:]...)
				rf.mu.Unlock()
				return
			}
			var reply RequestVoteReply
			err = client.Call("Raft.ReplyVote", args, &reply)
			if err != nil {
				log.Println("RPC error: ", err)
				return
			}

			if rf.CollectVotes(&reply) {
				mu.Lock()
				cancelled = true
				mu.Unlock()
				return
			} else {
				rf.mu.Lock()
				if rf.currentRole == Follower {
					fmt.Println("结束收集投票")
					rf.mu.Unlock()
					return
				} else {
					rf.mu.Unlock()
				}
			}
		}(peer, i)
	}

	wg.Wait()

	// 无论是否选举成功都结束选举定时
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Stop()

	if cancelled {
		fmt.Println("选举成功")
		rf.httpListen()
		for _, follower := range rf.peers {
			if follower == "localhost:"+rf.id {
				continue
			}
			rf.sentLength[follower] = len(rf.Logs)
			rf.ackedLength[follower] = 0
			fmt.Println("replicatedLogs", follower)
		}
	} else {
		fmt.Println("选举失败")
		rf.currentRole = Follower

		if rf.currentLeader == rf.id {
			rf.currentLeader = "null"
		}

		if rf.votedFor == rf.id {
			rf.votedFor = "null"
		}
		rf.voteReceived = []string{}
	}
}

// RAFT 2
func (rf *Raft) ReplyVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("节点 %s 收到来自 %s 的投票请求\n", rf.id, args.NodeId)

	myLogTerm := 0
	if len(rf.Logs) > 0 {
		myLogTerm = rf.Logs[len(rf.Logs)-1].Term
	}
	logOK := args.LastLogTerm > myLogTerm || (args.LastLogTerm == myLogTerm && args.LastLogIndex >= len(rf.Logs))
	termOK := args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == "null" || rf.votedFor == args.NodeId))

	if termOK && logOK {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = args.NodeId
		fmt.Print("接受投票\n")
		reply.VoteGranted = true
	} else {
		fmt.Print("拒绝投票\n")
		reply.VoteGranted = false
	}
	reply.NodeId = rf.id
	reply.Term = rf.currentTerm
	return nil
}

// RAFT 3
func (rf *Raft) CollectVotes(reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole == Candidate && reply.Term == rf.currentTerm && reply.VoteGranted {
		rf.voteReceived = append(rf.voteReceived, reply.NodeId)
		if len(rf.voteReceived) > len(rf.peers)/2 {
			rf.currentRole = Leader
			rf.currentLeader = rf.id
			fmt.Println("已获得超过二分之一票数")
			return true
		}
	} else if reply.Term > rf.currentTerm {
		fmt.Println("存在更新的 Term")
		rf.currentTerm = reply.Term
		rf.currentRole = Follower
		rf.votedFor = "null"

		return false
	}
	return false
}
