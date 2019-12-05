package nsqd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

// 管理所有 TCP 连接
// 通过实现 Handler 接口来对新连接进行处理
type tcpServer struct {
	ctx   *context
	conns sync.Map   // 这里使用了 syncmap，用 Addr 作为 key，Conn 作为 value
}

// nsqd 中对新 TCP 连接的处理函数
func (p *tcpServer) Handle(clientConn net.Conn) {
	// 新的连接打印对方地址端口
	p.ctx.nsqd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 客户端通过发送四个字节的消息来初始化自己，这里包括它通信的协议版本，这使得我们可以将协议从 test or line 更换到任何内容
	buf := make([]byte, 4)
	// 获取协议版本
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)
	// 打印协议版本，v1.2.0的协议版本打印出来的是 "  V2"
	p.ctx.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)
	// 创建对应的协议管理器
	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		// 使用 V2 版本的协议操作来进行处理，创建对应的版本管理器即可
		prot = &protocolV2{ctx: p.ctx}
	default:
		// 暂时仅支持 V2 一个版本，其余的版本直接断开连接并返回
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	// 将连接加入到 TCP 服务器的 sync.map 中
	p.conns.Store(clientConn.RemoteAddr(), clientConn)
	// 协议定义的操作，开始运行这个客户端的连接
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
	}
	// 连接断开
	p.conns.Delete(clientConn.RemoteAddr())
}

// nsqd 退出时调用
func (p *tcpServer) CloseAll() {
	// 遍历 sync.map 进行 close 操作
	p.conns.Range(func(k, v interface{}) bool {
		v.(net.Conn).Close()
		return true
	})
}
