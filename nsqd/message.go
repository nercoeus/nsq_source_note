package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// 消息定义
const (
	MsgIDLength       = 16    // ID 长度
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte
// 消息结构体
type Message struct {
	ID        MessageID // 消息 ID，topic 负责生成
	Body      []byte    // 消息体
	Timestamp int64     // 消息生成时间戳
	Attempts  uint16    // 尝试次数

	// 空中处理(投递出去,还没有收到确认回复)
	// for in-flight handling
	deliveryTS time.Time // 这个字段怪怪的
	clientID   int64     // 客户端 ID
	pri        int64     // 待确认时间
	index      int       // 消息在优先队列 inFlightPqueue 中的位置
	deferred   time.Duration
}

// 生成一个消息
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),  // 当前毫秒时间戳
	}
}

// 写入消息
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64
	// 消息格式见下面
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))
	// 写入 buf: timestamp + attempts
	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}
	// 写入消息 ID
	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}
	// 写入消息体
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
// 解消息
func decodeMessage(b []byte) (*Message, error) {
	var msg Message
	// 长度错误
	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}
	// 拿出时间戳和尝试次数
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	// 消息 ID
	copy(msg.ID[:], b[10:10+MsgIDLength])
	// 消息体
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

// writeMessageToBackend 将消息写入磁盘中
func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	// 将 msg 写入 buffer 中
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	// 永久 topic 的 backend 调用 Put 方法进行存盘
	// 临时 topic 的 dummyBackendQueue 会直接返回 nil
	return bq.Put(buf.Bytes())
}
