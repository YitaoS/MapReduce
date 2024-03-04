package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

// Logger 结构体
type Logger struct {
	mu sync.Mutex // 用于确保写操作的线程安全
}

var (
	logger *Logger
	once   sync.Once
)

func getLogger() *Logger {
	once.Do(func() {
		logger = &Logger{}
	})
	return logger
}

// Log 方法用于写日志
func (l *Logger) Log(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 获取当前时间
	currentTime := time.Now().Format("2024-01-02 15:04:05")
	// 获取调用函数的信息
	pc, _, _, ok := runtime.Caller(1)
	var funcName string
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
	}

	// 构建日志内容
	logContent := fmt.Sprintf("[%s] [Func %s] %s\n", currentTime, funcName, message)

	// 写入文件
	file, err := os.OpenFile("/home/ys386/MapReduce/6.5840/src/raft/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(logContent); err != nil {
		log.Fatal(err)
	}
}
