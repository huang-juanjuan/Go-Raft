package main

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// getRequest 处理 HTTP 请求的回调函数
func (rf *Raft) getRequest(writer http.ResponseWriter, request *http.Request) {
	// 解析请求的表单数据
	request.ParseForm()

	// 检查请求的 URL 是否包含 "message" 参数，并且当前有领导者节点
	// 例如，http://localhost:8080/req?message=ohmygod
	if len(request.Form["message"]) > 0 && rf.currentLeader != "null" {
		message := request.Form["message"][0] // 提取消息内容
		m := LogEntry{
			Command: message, // 创建一个日志条目
		}
		rf.Logs = append(rf.Logs, m)
		// 接收到消息后，直接将其转发给领导者节点进行处理
		fmt.Println("http监听到了消息")
		writer.Write([]byte("ok!!!")) // 向客户端返回确认消息
	}
}

var HandleFuncFlag = true

func (rf *Raft) httpListen() {
	// 创建一个 http.Server 实例
	rf.server = &http.Server{
		Addr:    ":8080",
		Handler: nil, // 使用默认的 http.DefaultServeMux
	}

	if HandleFuncFlag {
		HandleFuncFlag = false
		http.HandleFunc("/req", rf.getRequest)
	}
	go func() {
		fmt.Println("监听8080")
		if err := rf.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("Server error:", err)
		}
	}()
}

func (rf *Raft) stopListening() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := rf.server.Shutdown(ctx); err != nil {
		fmt.Println("Shutdown error:", err)
	} else {
		fmt.Println("监听已停止")
	}
}
