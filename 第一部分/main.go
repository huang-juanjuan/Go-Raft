package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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

	select {}
}
