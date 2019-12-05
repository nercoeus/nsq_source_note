package nsqd

// BackendQueue represents the behavior for the secondary message
// storage system
// 用来辅助消息的存储，有两种实现，一种临时对象使用，直接丢弃。一种用来将多余的数据存储在磁盘上
type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}
