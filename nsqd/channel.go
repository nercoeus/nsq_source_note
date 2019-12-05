package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

// 消费者,客户端实现
type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64 // 重新投递消息数
	messageCount uint64 // topic 发送过来消息数
	timeoutCount uint64 // 超时数

	sync.RWMutex

	topicName string   // topic name
	name      string   // channel name
	ctx       *context // 上下文

	backend BackendQueue // 存盘机制

	memoryMsgChan chan *Message // 消息队列
	exitFlag      int32         // 是否退出
	exitMutex     sync.RWMutex  // 用来对 exitFlag 加锁

	// state tracking
	clients        map[int64]Consumer // 所有客户端
	paused         int32              // 是否暂停
	ephemeral      bool               // 是否是临时 channel
	deleteCallback func(*Channel)     // 删除回调函数
	deleter        sync.Once          // 保证回调函数仅执行一次

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	// 延迟队列的三个字段
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	// 待确认队列的三个字段
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
// 创建一个 channel 并返回对应指针
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	// 消息队列,默认 10000
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		c.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}
	// 打开性能监视器
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}
	// 初始化延迟队列和待确认队列
	c.initPQ()
	// 判断是不是暂时 channel,选择过量消息存储方式
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	// 通知 lookupd 创建了 channel
	c.ctx.nsqd.Notify(c)

	return c
}

// 初始化两个队列，均使用优先队列
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))
	// 在同一个结构体中使用多个锁来细化粒度虽然
	c.inFlightMutex.Lock()
	// 代表正在投递但是还没有投递成功的消息
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	// 延迟消息和投递失败的消息
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
// 返回 channel 是否关闭或者退出（不可用状态）
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
// 删除 chanel
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
// 关闭 channel
func (c *Channel) Close() error {
	return c.exit(false)
}

// 删除或关闭 channel
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()
	// 设置不可用状态
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	// 所有客户端均关闭
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		// empty the queue (deletes the backend files, too)
		// 删除的话需要删除所有未发送消息
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	// 仅关闭的话，将所有消息放入磁盘中
	c.flush()
	return c.backend.Close()
}

// 清空所有队列中的下消息
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()
	// 重置两个队列
	c.initPQ()
	// 清空所有客户端
	for _, client := range c.clients {
		client.Empty()
	}
	// 清空所有未接收消息
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
// 将消息全部落盘
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}
	// 队列中的消息落盘
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	c.inFlightMutex.Lock()
	// 存盘待确认的消息
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	// 存盘延时消息
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

// 消息长度
func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

// 暂停 channel
func (c *Channel) Pause() error {
	return c.doPause(true)
}

// 恢复 channel
func (c *Channel) UnPause() error {
	return c.doPause(false)
}

// 暂停 channel
func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	// 暂停或恢复所有 client
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

// 是否暂停
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
// 向 channel 中投递一条消息，在 topic 的消息循环中投递
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	// channel 是否可用
	if c.Exiting() {
		return errors.New("exiting")
	}
	// 调用 put
	err := c.put(m)
	if err != nil {
		return err
	}
	// 记录 msgCount
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 将消息推送出去
func (c *Channel) put(m *Message) error {
	select {
	// 存入消息队列中
	case c.memoryMsgChan <- m:
	default:
		// 消息队列满了就存入 backend 队列中
		b := bufferPoolGet()
		// 存入磁盘
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// 准备发送延迟消息,先添加到延时队列,时间到再 put
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message
// 重置待确认队列中的超时消息
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 最长确认时间 15min
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	// 重新加入待确认队列,仅仅修改了超时时间
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
// 接收消息确认并把消息从待确认队列删除
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 统计一下性能,消息确认时间
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
// 重置一条消息,并判断时候需要延迟发送,以投递消息,但是客户端请求重新投递(它不想处理...)
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 消息数加一
	atomic.AddUint64(&c.requeueCount, 1)
	// 立即投递
	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	// 添加到延迟推送队列
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// 增加一个客户端
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return nil
	}
	// 客户端过多的话报错,默认不设限制
	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}

	c.clients[clientID] = client
	return nil
}

// RemoveClient removes a client from the Channel's client list
// 删除一个客户端
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)
	// 删除 channel
	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// 推送一条消息,等待确认,timeout 是确认超时时间
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	// 加到 InFlightMessage 中
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	// 记录进优先队列
	c.addToInFlightPQ(msg)
	return nil
}

// 准备添加延迟消息
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	// 获取推送时间
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	// 将消息存入 deferredMessage
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	// 将消添加到 deferredPQ 优先队列中
	c.addToDeferredPQ(item)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
// 在 inFlightMessages 中存入一条消息
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
// 原子操作:移出一条已经确认的消息
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	// 先拿出消息
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	// 删除
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

// 添加到 inFlightPQ 中
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

// 从 inFlightPQ 中删除一条消息,直接传入下标即可
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// 向 deferredMessages 中存入一条消息
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	// 加锁存入
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

// 从 deferredMessages 中取出一条消息
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	// 删除即可
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

// 添加到 deferredPQ 中
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// 在 NSQD 协程中进行处理,传入当前时间
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		// 等待时间到,可以投递
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		// 将消息取出来并进行 put
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}

exit:
	return dirty
}

// 在 NSQD 协程中进行处理,传入当前时间
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		// 判断又消息到时间了吗
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true
		// 清空到时消息
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		// 这条消息超时没有确认
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			// 消息超时后的处理客户端
			client.TimedOutMessage()
		}
		// 重新投递消息
		c.put(msg)
	}

exit:
	return dirty
}
