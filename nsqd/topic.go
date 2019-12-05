package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

// topic 结构体
type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// 这两个字段仅作统计信息
	messageCount uint64 // topic 接受的消息数
	messageBytes uint64 // 所有接收消息结构体的长度

	sync.RWMutex // 在 channel 的操作需要加锁，包括 putMessage

	name              string                // topic 的 name
	channelMap        map[string]*Channel   // 存储所有 channel
	backend           BackendQueue          // 当消息数过多，消息队列中存不下会将消息存入 backend
	memoryMsgChan     chan *Message         // 内存中存放的消息队列，默认长度是 10000
	startChan         chan int              // 接收开始信号的 channel，调用 start 开始 topic 消息循环
	exitChan          chan int              // 判断 topic 是否退出
	channelUpdateChan chan int              // channel 更新时用来通知并更新消息循环中的 chan 数组
	waitGroup         util.WaitGroupWrapper // 用来防止主进程退出并终止子协程
	// 在 select 的地方都要添加 exitChan，除非使用 default 或者保证程序不会永远阻塞在 select 处
	exitFlag          int32                 // topic 退出标识符
	idFactory         *guidFactory          // 生成 guid 的工厂方法

	ephemeral      bool         // 该 topic 是否是临时 topic
	deleteCallback func(*Topic) // topic 删除时的回调函数
	deleter        sync.Once    // 确保 deleteCallback 仅执行一次

	paused    int32    // topic 是否暂停
	pauseChan chan int // 改变 topic 暂停/运行状态

	ctx *context // topic 的上下文
}

// Topic constructor
// 创建一个新的 topic
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     nil,
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		// 所有 topic 使用同一个 guidFactory，因为都是用的 nsqd 的 ctx.nsqd.getOpts().ID
		idFactory:         NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	// 根据消息队列生成消息chan
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		t.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}
	// 判断这个 topic 是不是暂时的，暂时的 topic 消息仅仅存储在内存中
	// DummyBackendQueue 和 diskqueue 均实现了 backend 接口
	if strings.HasSuffix(topicName, "#ephemeral") {
		// 临时的 topic，设置标志并使用 newDummyBackendQueue 初始化 backend
		t.ephemeral = true
		t.backend = newDummyBackendQueue()   // 实现了 backend 但是并没有逻辑，所有操作仅仅返回 nil
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// 使用 diskqueue 初始化 backend
		t.backend = diskqueue.New(
			topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	// 使用一个新的协程来执行 messagePump
	t.waitGroup.Wrap(t.messagePump)
	// 调用 Notify
	t.ctx.nsqd.Notify(t)

	return t
}

// 开始消息循环
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
// 判断 topic 是否退出/关闭
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// 获取一个 channel 没有就创建一个
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	// 这里监听 exitChan 是一个值得探讨的点，通过监听 exitChan 可以获取 channelUpdateChan 是否关闭
	// 从而控制是否向 channelUpdateChan 中发送消息
	// 否则可能，channelUpdateChan 的对端已经不再监听，但是程序会阻塞在向 channelUpdateChan 发送消息不能释放
	if isNew {
		// update messagePump state
		select {
		// 创建新的 channel 就更新 topic
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
// 获取或创建一个 Channel 这个函数期望调用者加锁
// 这个函数仅由内部进行调用，例如 GetChannel 调用
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	// 获取 channel
	channel, ok := t.channelMap[channelName]
	if !ok {
		// 没有就创建
		deleteCallback := func(c *Channel) {
			// channel 删除函数，用回调的方式添加到 channel 中
			t.DeleteExistingChannel(c.name)
		}
		// 创建新的 channel
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		// 加入 channel map
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	// 存在直接返回
	return channel, false
}

// 获取一个 channel 不存在报错
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
// channel 删除的回调函数，仅在 channel 存在时才进行删除
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	// 从 channelMap 中删除这个 channel
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	// 删除 channel，会先清空 channel 中的 msg 再删除
	channel.Delete()

	// update messagePump state
	// 更新 channel
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	// 当临时 topic 中不存在 channel 时，将其删除
	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
// 向 topic 中写入一条消息
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	// 调用 put
	err := t.put(m)
	if err != nil {
		return err
	}
	// 增加消息计数
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

// PutMessages writes multiple Messages to the queue
// 向 topic 中写入多条消息
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		// 调用 put 添加 msg
		err := t.put(m)
		if err != nil {
			// 增加消息计数
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		// 记录所有消息长度
		messageTotalBytes += len(m.Body)
	}
	// 增加消息计数
	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 投递一条消息到 channel 中
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m:   // 内存中的消息队列还可以继续存放，直接放入 memoryMsgChan 交给 messagePump 处理即可
	default:
		// 内存中消息队列存放满了，需要存放到 backend 中
		// 临时的 topic 会直接进行丢弃，永久的则会存放到磁盘中
		b := bufferPoolGet() // 从 buffer pool 中获取一个 buffer
		// 调用 writeMessageToBackend 将消息写入磁盘中
		err := writeMessageToBackend(b, m, t.backend)
		// 归还 buffer 对象
		bufferPoolPut(b)
		// 将错误存储在 nsqd 中
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

// topic 中所有
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
// 负责消息循环，将进入 topic 中的消息投递到 channel 中
// 仅在 topic 创建时通过新的协程开始执行
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	// 仅在触发 startChan 开始运行，否则会阻塞住，直到退出
	for {
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		case <-t.startChan:
		}
		break
	}
	t.RLock()
	// 获取当前 topic 的所有 channel
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	// 设置消息循环接收 channel
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// main message loop
	for {
		select {
		case msg = <-memoryMsgChan:   // 内存中的消息队列，长度由配置文件设置默认：10000
		case buf = <-backendChan:
			// 内存满了，将消息放到磁盘中，磁盘中的消息需要解数据才能使用
			// 解消息的消息结构见 writeMessageToBackend
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan:
			// channel 的更新，清空后重新获取。。。
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			// 更新一下监听 channel 即可，没有 channel 就暂停
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan:
			// 可能是暂停 topic 也可能是开启 topic
			if len(chans) == 0 || t.IsPaused() {
				// 暂停 topic
				memoryMsgChan = nil
				backendChan = nil
			} else {
				// 恢复 topic
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			// 退出 topic 消息循环
			goto exit
		}
		// 遍历所有 channel，在 memoryMsgChan 触发和 backendChan 解消息成功时执行
		for i, channel := range chans {
			// msg 就是那个消息
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			// 每个 channel 都需要唯一的实例，第一个 channel 不需要复制，已经有一份了(msg)，稍微优化
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			// 延迟消息，使用 PutMessageDeferred 投递
			if chanMsg.deferred != 0 {
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			// 否则直接投递到 channel 即可
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
// 删除 topic 并关闭所有 channel
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
// 关闭存储所有未完成的 topic data 并关闭所有 channel
func (t *Topic) Close() error {
	return t.exit(false)
}

// topic 删除和关闭均调用这个函数
func (t *Topic) exit(deleted bool) error {
	// 打开退出标志
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}
	// 是否删除
	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		// 需要在 lookupd 中删除这个 topic
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}
	// 关闭 topic 所有 channel，真好用
	close(t.exitChan)

	// synchronize the close of messagePump()
	// 同步等待 messagePump 关闭
	t.waitGroup.Wait()

	if deleted {
		// 删除的话，先清空 channelMap
		t.Lock()
		for _, channel := range t.channelMap {
			// 手动释放了
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		// 删除所有未投递的消息
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	// 关闭所有 channel，并不删除
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	// 将 channel 中未投递的数据全部保存至磁盘中
	t.flush()
	return t.backend.Close()
}

// 清空未投递的消息
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
			// 把所有消息队列中的消息删除，接受但不处理
		default:
			goto finish
		}
	}

finish:
	// 最后清除 backend 中的消息
	return t.backend.Empty()
}

// 将内存中的未投递消息保存至磁盘中
func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			// 使用 writeMessageToBackend 写入磁盘即可
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

// 用于性能统计
func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

// 暂停 topic 消息循环
func (t *Topic) Pause() error {
	return t.doPause(true)
}

// 恢复 topic 消息循环
func (t *Topic) UnPause() error {
	return t.doPause(false)
}

// 更改 topic 暂停/运行状态
func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	// 发送 1 激活 topic 暂停/恢复过程，具体标志由 t.paused 标识
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

// 判断 topic 是否暂停消息循环
func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

// 生成唯一 ID，仅用于该 topic 的消息
func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}
