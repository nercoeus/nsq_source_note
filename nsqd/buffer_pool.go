package nsqd

import (
	"bytes"
	"sync"
)

// 用来做 buffer pool
var bp sync.Pool

// 初始化 buffer pool
func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

// 封装 pool.Get() 获取一个对象
func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

// 封装 pool.Put()
func bufferPoolPut(b *bytes.Buffer) {
	bp.Put(b)
}
