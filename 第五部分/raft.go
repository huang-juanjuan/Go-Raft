package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Role 是一个新的类型，表示 Raft 节点的角色
type Role int

// 定义 Role 类型的常量
const (
	Follower Role = iota
	Candidate
	Leader
)

// 定义请求和响应结构
type AddPeerArgs struct {
	NewPeer string
}

type AddPeerReply struct {
	Peers []string
}

// Raft 结构
type Raft struct {
	mu sync.RWMutex // 互斥锁
	id string       // 端口号

	peers         []string   // 所有节点的端口号
	currentTerm   int        // 当前 Term 号
	currentRole   Role       // 当前角色
	currentLeader string     // 当前 Leader 的端口号
	Logs          []LogEntry // 日志
	CommitLength  int

	votedFor     string   // 当前 Term 中给谁投了票
	voteReceived []string // 收到的同意投票的端口号

	sentLength  map[string]int // 每个节点日志复制的插入点
	ackedLength map[string]int // 每个节点已接收的日志长度
	server      *http.Server

	electionTimer     *time.Timer  // 选举定时器
	heartBeatTimer    *time.Ticker // 心跳计时器
	lastHeartBeatTime int64        // 上次收到心跳的时间
	timeout           int          // 心跳超时时间
}

// NewRaft 创建并初始化一个新的 Raft 实例
func NewRaft(id string) *Raft {
	rf := &Raft{
		id:                id,
		peers:             []string{},
		currentTerm:       0,
		currentRole:       Follower,
		currentLeader:     "null",
		Logs:              []LogEntry{},
		votedFor:          "null",
		voteReceived:      []string{},
		CommitLength:      0,
		sentLength:        make(map[string]int),
		ackedLength:       make(map[string]int),
		electionTimer:     time.NewTimer(time.Duration(rand.Intn(5000)+5000) * time.Millisecond),
		heartBeatTimer:    time.NewTicker(1000 * time.Millisecond),
		lastHeartBeatTime: time.Now().UnixMilli(),
		timeout:           5,
	}
	return rf
}

// RAFT 1
func (rf *Raft) initRaft() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.currentRole = Candidate
	rf.votedFor = rf.id
	rf.voteReceived = []string{rf.id}
}

func (rf *Raft) AddPeer(args *AddPeerArgs, reply *AddPeerReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查是否已经存在该 peer
	for _, peer := range rf.peers {
		if peer == args.NewPeer {
			fmt.Printf("Node %s: Peer %s already exists\n", rf.id, args.NewPeer)
			reply.Peers = rf.peers
			return nil
		}
	}

	// 如果不存在则添加
	rf.peers = append(rf.peers, args.NewPeer)
	if rf.currentRole == Leader {
		rf.sentLength[args.NewPeer] = len(rf.Logs)
		rf.ackedLength[args.NewPeer] = 0
		go rf.Replicating(args.NewPeer)
	}
	reply.Peers = rf.peers

	// 打印更新后的 peers 列表
	fmt.Printf("Node %s updated peers: %v\n", rf.id, rf.peers)
	return nil
}

func (rf *Raft) resetElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	timeout := time.Duration(rand.Intn(5000)+5000) * time.Millisecond
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(timeout)
}

func (rf *Raft) electionTimerStart() {

	for range rf.electionTimer.C {
		rf.mu.Lock()
		if rf.currentRole != Candidate {
			rf.mu.Unlock()
			continue
		}
		fmt.Println("选举超时")
		rf.mu.Unlock()
		rf.initRaft()
	}
}

func (rf *Raft) heartBeatTimerStart() {
	for range rf.heartBeatTimer.C {
		rf.mu.Lock()
		if rf.currentRole != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		rf.SendHeartBeat()
	}
}
