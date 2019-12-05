package nsqd

// the core algorithm here was borrowed from:
// Blake Mizerany's `noeqd` https://github.com/bmizerany/noeqd
// and indirectly:
// Twitter's `snowflake` https://github.com/twitter/snowflake

// only minor cleanup and changes to introduce a type, combine the concept
// of workerID + datacenterId into a single identifier, and modify the
// behavior when sequences rollover for our specific implementation needs

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// GUID 相关参数
const (
	nodeIDBits     = uint64(10)
	sequenceBits   = uint64(12)
	nodeIDShift    = sequenceBits
	timestampShift = sequenceBits + nodeIDBits
	sequenceMask   = int64(-1) ^ (int64(-1) << sequenceBits)  // 4095

	// ( 2012-10-28 16:23:42 UTC ).UnixNano() >> 20
	twepoch = int64(1288834974288)
)

var ErrTimeBackwards = errors.New("time has gone backwards")
var ErrSequenceExpired = errors.New("sequence expired")
var ErrIDBackwards = errors.New("ID went backward")

type guid int64

// 用来生成唯一的 GUID
type guidFactory struct {
	sync.Mutex // 防止冲突

	nodeID        int64 // 同一个 nsqd 的 topic 的 guidFactory 中的 nodeID 均相同
	sequence      int64 // 一毫秒生成 ID 个数
	lastTimestamp int64 // 上一次的时间戳
	lastID        guid  // 上一个 ID，确保 ID 递增
}

// GUID 工厂
func NewGUIDFactory(nodeID int64) *guidFactory {
	return &guidFactory{
		nodeID: nodeID,
	}
}

// 新的 GUID 号
func (f *guidFactory) NewGUID() (guid, error) {
	f.Lock()

	// divide by 1048576, giving pseudo-milliseconds
	// 除以 1048576，得到当前毫秒数
	ts := time.Now().UnixNano() >> 20
	// 时间混乱了
	if ts < f.lastTimestamp {
		f.Unlock()
		return 0, ErrTimeBackwards
	}
	// 最新消息时间毫秒数 = 当前，根据 sequence 计数器区分
	if f.lastTimestamp == ts {
		f.sequence = (f.sequence + 1) & sequenceMask
		// 如果冲突了，sequence 至少为 1,如果为 0，则表明 sequence >= 4096,设置不能每毫秒无线生成
		if f.sequence == 0 {
			f.Unlock()
			return 0, ErrSequenceExpired
		}
	} else {
		f.sequence = 0
	}
	// 更新最后获取时间
	f.lastTimestamp = ts
	// id = [ 22位ts + 10位 workerId + 12位 sequence ]
	id := guid(((ts - twepoch) << timestampShift) |
		(f.nodeID << nodeIDShift) |
		f.sequence)
	// 因为根据 ts 为头部获取的 ID 所以，不存在后面的 ID <= 前面 ID 的情况发生
	// 除非在极短时间内超过了 sequenceMask 数并继续生成才会小于当前值
	// 这样的话在逻辑中会循环生成 ID，一等到这一毫秒过去即可获取正确的 ID
	if id <= f.lastID {
		f.Unlock()
		return 0, ErrIDBackwards
	}

	f.lastID = id

	f.Unlock()

	return id, nil
}

// 将获取的 ID 转换为 MessageID
func (g guid) Hex() MessageID {
	var h MessageID
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h[:], b[:])
	return h
}
