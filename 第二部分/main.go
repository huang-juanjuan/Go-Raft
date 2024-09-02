package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

func (rf *Raft) RegisterNode(args *AddPeerArgs, reply *AddPeerReply) error {
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
	fmt.Printf("Node %s updated peers: %v\n", rf.id, rf.peers)

	reply.Peers = rf.peers
	for _, peer := range rf.peers {
		if peer == args.NewPeer || peer == "localhost:"+rf.id {
			continue
		}
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			log.Println("Dialing:", err)
			continue
		}

		addPeerArgs := &AddPeerArgs{NewPeer: args.NewPeer}
		var addPeerReply AddPeerReply
		err = client.Call("Raft.AddPeer", addPeerArgs, &addPeerReply)
		if err != nil {
			log.Println("RPC error:", err)
		}
	}
	return nil
}

func startRaftServer(rf *Raft, port string) {
	err := rpc.Register(rf)
	if err != nil {
		log.Panic(err)
	}

	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	go http.Serve(listener, nil)
}

func main() {
	portPtr := flag.String("port", "0", "端口号")
	centralNodePtr := flag.String("central", "localhost:8040", "中心节点(默认 localhost:8040 )")
	flag.Parse()

	if *portPtr == "0" {
		fmt.Println("请确认端口号")
		os.Exit(1)
	}

	rf := NewRaft(*portPtr)
	go startRaftServer(rf, *portPtr)

	client, err := rpc.DialHTTP("tcp", *centralNodePtr)
	if err != nil {
		log.Fatalf("Error connecting to central node: %s", err)
	}
	args := &AddPeerArgs{NewPeer: "localhost:" + *portPtr}
	var reply AddPeerReply
	err = client.Call("Raft.RegisterNode", args, &reply)
	if err != nil {
		log.Fatalf("RPC error: %s", err)
	}

	rf.mu.Lock()
	rf.peers = reply.Peers
	fmt.Printf("Node %s initial peers: %v\n", rf.id, rf.peers)
	rf.mu.Unlock()

	go rf.electionTimerStart()
	rf.electionTimer.Stop()

Circle:
	for {
		if rf.becomeCandidate() {
			// 成为候选人节点后，向其他节点请求选票进行选举
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				NodeId:       rf.id,
				LastLogIndex: len(rf.Logs),
				LastLogTerm:  0,
			}
			if len(rf.Logs) > 0 {
				args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
			}
			rf.mu.Unlock()

			rf.startElection(args)

			rf.mu.Lock()
			if rf.currentRole == Leader {
				fmt.Printf("Node %s has become the leader\n", rf.id)
				rf.mu.Unlock()
				break
			} else {
				rf.mu.Unlock()
			}
		} else {
			break
		}
	}

	// 进行心跳检测
	for {
		// 5秒检测一次
		time.Sleep(time.Millisecond * 5000)
		rf.mu.Lock()

		if rf.currentRole != Leader && rf.lastHeartBeatTime != 0 && (time.Now().UnixMilli()-rf.lastHeartBeatTime) > int64(rf.timeout*1000) {
			fmt.Printf("心跳检测超时，已超过%d秒\n", rf.timeout)
			fmt.Println("即将重新开启选举")
			rf.currentRole = Follower
			rf.currentLeader = "null"
			rf.votedFor = "null"
			rf.voteReceived = []string{}
			rf.lastHeartBeatTime = 0
			rf.mu.Unlock()
			goto Circle
		} else {
			rf.mu.Unlock()
		}
	}
}
