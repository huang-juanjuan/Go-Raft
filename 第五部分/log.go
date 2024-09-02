package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type LogEntry struct {
	Term    int
	Index   int
	Command string
}

type LogRequestArgs struct {
	LeaderId     string     // Leader 的 Id
	CommitLength int        // Leader 已经提交的日志
	Term         int        // Leader 当前 Term 号
	LogLength    int        // 日志长度
	LogTerm      int        // 日志复制点的 Term
	Entries      []LogEntry // 日志列表
}

type LogReplyArgs struct {
	NodeId      string // Follower 的 Id
	CureentTerm int    // Foller 当前的 Term
	Ack         int    // 接收复制后的日志长度
	Flag        bool   // 是否接收复制
}

// Raft 4
func (rf *Raft) Boradcast(newLog LogEntry) {
	rf.mu.Lock()
	if rf.currentRole == Leader {
		newLog.Term = rf.currentTerm
		newLog.Index = len(rf.Logs) + 1
		rf.Logs = append(rf.Logs, newLog)
		// 添加日志
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			peer := rf.peers[i]
			if peer == "localhost:"+rf.id {
				continue
			}
			go rf.Replicating(peer)
		}
	} else {
		rf.mu.Unlock()
	}
}

// Raft 5
func (rf *Raft) Replicating(peer string) error {
	i := rf.sentLength[peer]
	ei := len(rf.Logs) - 1
	prevLogTerm := 0

	var entries []LogEntry
	if ei >= i {
		entries = rf.Logs[i : ei+1]
	} else {
		entries = []LogEntry{}
	}

	if i > 0 {
		prevLogTerm = rf.Logs[i-1].Term
	}

	client, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		log.Println("Dialing error: ", err)
		return nil
	}

	var reply LogReplyArgs
	args := &LogRequestArgs{
		LeaderId:     "localhost:" + rf.id,
		CommitLength: rf.CommitLength,
		Term:         rf.currentTerm,
		LogLength:    i,
		LogTerm:      prevLogTerm,
		Entries:      entries,
	}
	err = client.Call("Raft.Replying", args, &reply)
	if err != nil {
		log.Println("RPC error: ", err)
		return nil
	}

	return nil
}

// Raft-6
func (rf *Raft) Replying(args *LogRequestArgs, reply *LogReplyArgs) error {
	// 锁定 Raft 实例以防止并发修改
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求中的任期比当前节点的任期更高，更新任期并将当前角色设为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = "null"
		rf.currentRole = Follower
		rf.currentLeader = args.LeaderId
	}

	// 如果任期相同且当前节点角色为 Candidate，更新角色为 Follower
	if args.Term == rf.currentTerm && rf.currentRole == Candidate {
		rf.currentRole = Follower
		rf.currentLeader = args.LeaderId
	}

	// 验证日志条目
	logOk := (len(rf.Logs) >= args.LogLength) && (args.LogLength == 0 || args.LogTerm == rf.Logs[args.LogLength-1].Term)

	// 准备响应
	reply.NodeId = "localhost:" + rf.id
	reply.CureentTerm = rf.currentTerm // 注意：此处 `CureentTerm` 可能有拼写错误，建议修正为 `CurrentTerm`
	reply.Ack = 0
	reply.Flag = false

	// 如果任期相同且日志条目有效，追加日志并设置确认号和标志
	if args.Term == rf.currentTerm && logOk {
		rf.AppendEntries(args.LogLength, args.CommitLength, &args.Entries)
		ack := args.LogLength + len(args.Entries)
		reply.Ack = ack
		reply.Flag = true
	}
	client, err := rpc.DialHTTP("tcp", args.LeaderId)
	if err != nil {
		log.Println("Dialing error: ", err)
		return nil
	}
	flag := false
	err = client.Call("Raft.ReceivingAck", reply, &flag)
	if err != nil {
		log.Println("RPC error: ", err)
		return nil
	}
	return nil
}

// Raft 7
func (rf *Raft) AppendEntries(logLength int, leaderCommit int, entries *[]LogEntry) {
	if len(*entries) > 0 && len(rf.Logs) > logLength {
		if rf.Logs[logLength].Term != (*entries)[0].Term {
			rf.Logs = rf.Logs[:logLength-1]
		}
	}
	if logLength+len(*entries) > len(rf.Logs) {
		startIndex := len(rf.Logs) - logLength
		rf.Logs = append(rf.Logs, (*entries)[startIndex:]...)
	}

	if leaderCommit > rf.CommitLength {
		for i := rf.CommitLength; i < leaderCommit; i++ {
			fmt.Println("Follwer Commit Log ", i)
		}
		rf.CommitLength = leaderCommit
	}
}

// Raft 8
func (rf *Raft) ReceivingAck(reply *LogReplyArgs, flag *bool) error {
	if reply.CureentTerm == rf.currentTerm && rf.currentRole == Leader {
		if reply.Flag && reply.Ack >= rf.ackedLength[reply.NodeId] {
			rf.sentLength[reply.NodeId] = reply.Ack
			rf.ackedLength[reply.NodeId] = reply.Ack
			fmt.Println("开始提交日志")
			// go rf.CommitEntries()
		} else if rf.sentLength[reply.NodeId] > 0 {
			rf.sentLength[reply.NodeId] = rf.sentLength[reply.NodeId] - 1
			go rf.Replicating(reply.NodeId)
		}
	} else if reply.CureentTerm > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = reply.CureentTerm
		rf.currentRole = Follower
		rf.votedFor = "null"
		rf.mu.Unlock()
	}
	return nil
}
