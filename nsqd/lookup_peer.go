package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/lg"
)

// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
// 用来管理和 lookupd 之间的连接
type lookupPeer struct {
	logf            lg.AppLogFunc     // 日志等级信息
	addr            string            // lookupd 的地址
	conn            net.Conn          // 对应的连接 TCP 的
	state           int32             // 连接状态
	connectCallback func(*lookupPeer) // 连接成功的 callback
	maxBodySize     int64             // 最大存储大小
	Info            peerInfo          // peerInfo 初始化完成后由 lookupd 返回的数据进行填写
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
// 包含 lookPeer 信息
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// newLookupPeer creates a new lookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
// 创建一个 lookupdPeer 并连接到对应地址，连接成功会调用 connectCallback
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		logf:            l,                 // 日志信息
		addr:            addr,              // lookupd 的地址
		state:           stateDisconnected, // 初始状态为断开连接
		maxBodySize:     maxBodySize,       // 默认 5MB
		connectCallback: connectCallback,   // 连接回调函数
	}
}

// Connect will Dial the specified address, with timeouts
// nsqd 和 lookupd 进行连接,实现了 connect 接口
func (lp *lookupPeer) Connect() error {
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	// 进行 TCP 连接,一秒超时
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	// 保存连接
	lp.conn = conn
	return nil
}

// String returns the specified address
// 返回地址
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
// 实现了 Read 接口,添加了 1s 时间限制
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
// 实现了 Write 接口,添加了 1s 时间限制
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
// 实现 close 接口
func (lp *lookupPeer) Close() error {
	lp.state = stateDisconnected
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
// 执行传给 lookupd 的指令
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	// 初始状态
	initialState := lp.state
	if lp.state != stateConnected {
		// 先连接再说
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected
		// 尝试写入数据
		_, err = lp.Write(nsq.MagicV1)
		if err != nil {
			lp.Close()
			return nil, err
		}
		if initialState == stateDisconnected {
			// 初始连接需要调用 connectCallback
			lp.connectCallback(lp)
		}
		if lp.state != stateConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}
	if cmd == nil {
		return nil, nil
	}
	// 将消息写入连接
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	// 读取回复
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

// 读取连接数据
func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size 消息长度
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data
	// 读取消息
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
