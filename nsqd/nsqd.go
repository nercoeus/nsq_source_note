package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

// nsq 中的锁用的很好，锁要仅锁住特定的数据，而不是对过程加锁

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}

// nsqd 结构体
type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64 // 用来给每个客户端生成 ID，操作时使用原子操作

	sync.RWMutex // 仅在操作 topic 的时候才会使用这个锁

	opts atomic.Value // 配置信息结构体，所有配置信息都在这里，初始化时进行设置

	dl        *dirlock.DirLock // 文件夹锁
	isLoading int32            // 标识正在从文件中加载元数据
	errValue  atomic.Value     // 错误标识，使用原子 value
	startTime time.Time        // nsqd 启动时间

	topicMap map[string]*Topic // 管理所有的 topic

	clientLock sync.RWMutex     // 操作 Clients 时的读写锁
	clients    map[int64]Client // 管理所有的 Client

	lookupPeers atomic.Value // 用来记录所有 lookupPeer（和 lookupd 的连接）

	tcpServer     *tcpServer   // 用来管理所有连接
	tcpListener   net.Listener // 监听 TCP 连接
	httpListener  net.Listener // 监听 HTTP 连接
	httpsListener net.Listener // 监听 HTTPS 连接
	tlsConfig     *tls.Config  // 安全协议配置信息

	poolSize int // 用来运行 queueScanWorker 的协程数的多少

	notifyChan           chan interface{}      // 通知 lookupd 的管道
	optsNotificationChan chan struct{}         // 用来通知 lookupd IP 地址改变通知
	exitChan             chan int              // 用来通知各个 channel nsqd 退出从而停止循环监听
	waitGroup            util.WaitGroupWrapper // 用来防止主进程退出并终止子协程

	ci *clusterinfo.ClusterInfo // 专门用来和 lookupd 通信
}

// 根据配置根据配置创建一个新的 nsqd
func New(opts *Options) (*NSQD, error) {
	var err error
	// 设置数据缓存路径
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),              // 开始时间
		topicMap:             make(map[string]*Topic), // topic 管理
		clients:              make(map[int64]Client),  // 客户端 管理
		exitChan:             make(chan int),          // 退出通知管道
		notifyChan:           make(chan interface{}),  // 通知 lookupd 的管道
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),   // 目录锁，保证只有一个 nsqd 使用该目录
	}
	// 创建一个 HTTP 客户端，用来从 lookupd 中获取 topic 数据
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})
	// 保存初始化的配置
	n.swapOpts(opts)
	n.errValue.Store(errStore{})
	// 对目录加锁
	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("--data-path=%s in use (possibly by another instance of nsqd)", dataPath)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}
	// 数据统计的一些准备工作
	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}
	// 设置 tls config
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	// 用来管理所有 TCP 连接
	n.tcpServer = &tcpServer{}
	// TCP 监听接口
	n.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	// HTTP 监听接口
	n.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}
	// HTTPS 监听接口
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}

	return n, nil
}

// 获取配置信息
func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// 切换配置信息
func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// 通知 lookupd 配置文件信息的改变
func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

// 获取 TCP 地址
func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	return n.tcpListener.Addr().(*net.TCPAddr)
}

// 获取 HTTP 地址
func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

// 获取 HTTPS 地址
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	return n.httpsListener.Addr().(*net.TCPAddr)
}

// 设置 errValue 的值
func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

// 是否正常
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// 获取错误信息
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

// 获取 nsqd 健康标识，其实就是 GetError
func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

// 获取开始时间
func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// 添加新的客户端
func (n *NSQD) AddClient(clientID int64, client Client) {
	n.clientLock.Lock()
	n.clients[clientID] = client
	n.clientLock.Unlock()
}

// 删除一个客户端
func (n *NSQD) RemoveClient(clientID int64) {
	n.clientLock.Lock()
	_, ok := n.clients[clientID]
	if !ok {
		n.clientLock.Unlock()
		return
	}
	delete(n.clients, clientID)
	n.clientLock.Unlock()
}

// nsqd 开始运行
func (n *NSQD) Main() error {
	// 这里的 context 把 n 封装进去了
	ctx := &context{n}

	exitCh := make(chan error)
	var once sync.Once
	// 退出函数
	exitFunc := func(err error) {
		// 仅发送一次 err 给 exitch
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	n.tcpServer.ctx = ctx
	// 对 TCP 进行监听
	n.waitGroup.Wrap(func() {
		// 在 protocol 中包装了 TCPServer，传入 Listener、TCPHandler、AppLogFunc
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})
	// 开启对 HTTP 进行监听
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	n.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
	})
	// 开启对 HTTPS 进行监听
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}
	// 使用两个协程运行下面的方法
	n.waitGroup.Wrap(n.queueScanLoop)   // 延时消息的优先队列
	n.waitGroup.Wrap(n.lookupLoop)      // 如果 nsqd 发生变化，同步至 nsqloopdup，函数定义在 lookup 中
	if n.getOpts().StatsdAddress != "" {
		// 定时间 nsqd 的状态通过短链接的方式发送至一个状态监护进程中
		// 包括 nsqd 的应用资源信息，以及 nsqd 上 topic 的信息
		n.waitGroup.Wrap(n.statsdLoop)
	}

	err := <-exitCh
	return err
}

// 元数据结构体，json 类型,存储在 bat 文件中，记录所有 topic 和 channel 的信息
// 并在最后加上 nsqd 的版本号用于区分不同版本的 meta 结构
type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

// 创建新的 nsqd.bat 文件
func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

// 读取文件数据
func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

// 同步写入文件
func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// 加载 topic 和 channel 数据到 nsqd 中
func (n *NSQD) LoadMetadata() error {
	// 使用原子操作保证 isLoading 的数值变化
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)

	fn := newMetadataFile(n.getOpts())
	// 读取对应数据
	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	// 空文件
	if data == nil {
		return nil // fresh start
	}
	// 解数据
	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}
	// 循环处理 topic 数据
	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		// 保证 nsqd 中有这个 topic
		topic := n.GetTopic(t.Name)
		if t.Paused {
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			// 保证 topic 中有这个 channel
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		// 开始运行 topic
		topic.Start()
	}
	return nil
}

// 保留 topic 和 channel 信息
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)
	// 没啥说的，就是把所有 topic 以及它的所有 channel 信息保存至文件中，临时的不进行存储
	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	// 结尾加上版本号
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

// nsqd 的退出
func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		// 关闭 TCP 服务器
		n.tcpListener.Close()
	}
	if n.tcpServer != nil {
		// 退出时，将所有连接关闭
		n.tcpServer.CloseAll()
	}

	if n.httpListener != nil {
		// 关闭 HTTP 服务器
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		// 关闭 HTTPS 服务器
		n.httpsListener.Close()
	}

	n.Lock()
	// 将 topic 和 channel 信息存储起来
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	// 关闭所有 topic
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	// 关闭所有 goroutine
	close(n.exitChan)
	// 等待所有子协程退出
	n.waitGroup.Wait()
	// 解锁目录
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// 根据 topic 名称获取一个 topic 指针
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	// 该主题已经存在，加读锁进行获取
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()
	// 再进行一次判断， dobule check
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	// topic 删除时回调，仅执行一次（topic 保证）
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	// 创建一个新的 Topic 传入名字、context、删除回调函数
	t = NewTopic(topicName, &context{n}, deleteCallback)
	// 插入到 map 中
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if loading metadata at startup, no lookupd connections yet, topic started after load
	// 如果在启动时加载元数据，尚无 nsqdlookupd 连接，则在加载后启动 topic，由 LoadMetadata 自己启动即可
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}
	// 从下面开始就不再使用 NSQD 的全局锁转而使用 topic 的细粒度锁
	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	// 如果使用了 lookupd 就需要阻塞获取该 topic 的所有 channel，并立即进行创建
	// 这样可以确保将收到的任何消息缓冲到正确的通道
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		// 在所有 lookupd 中获取这个 topic 所有的 channels，这一步同样不需要在 LoadMetadata 时执行
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			// 临时的就不管了
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			// 否则需要创建 channel
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		// 没有 lookupd 地址但是配置文件中却有。。。
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	// 已经添加了所有 channel 可以开始 topic 下的消息分发
	// 向 startChan 发送通知 messagePump 开始运行
	t.Start()
	return t
}

// GetExistingTopic gets a topic only if it exists
// 仅在 topic 存在的时候进行获取
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
// 仅在 topic 存在的时候进行删除
// 这是 topic 删除的回调函数
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	// 详情见 delete()
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// 调用 PersistMetadata 在 topic 创建和删除时使用将 topic 和 chnnel 写入文件中
// 由 topic 和 channel 改变时进行调用，实时吧 nsqd 的元数据写入文件中
func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:   // 触发 loopupd 中的监听函数
			if !persist {
				return
			}
			n.Lock()
			// 写入文件
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
// 返回所有 topic 中的 channel
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
// 修改 pool（运行 queueScanWorker 的协程） 的大小
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	// num 是 channel 的总个数
	idealPoolSize := int(float64(num) * 0.25)
	// idealPoolSize 范围 [1,4]
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			// 关闭 queueScanWorker 协程直到剩余数量等于 idealPoolSize
			closeCh <- 1   // 一次只关闭一个
			n.poolSize--
		} else {
			// expand
			// 添加 queueScanWorker 协程，直到数量等于 idealPoolSize
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// 扫描 channel 的 deferredPQ 和 inFlightPQ  两个队列处理其中的消息
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		// 接收工作 channel
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			// 下面处理 channel 中 deferredPQ 和 inFlightPQ 两个消息优先队列
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			// 把 dirty 标志传回去用于统计
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
// 处理延迟消息的队列
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	// 默认 100ms 唤醒一次
	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	// 5s 刷新一次
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)
	// 所有 channel
	channels := n.channels()
	// 重新调整 pool 大小
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			// 每 5s 刷新 channel 并且更改 pool 的大小
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}
		// 默认最多扫描 20 个 channel
		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]  // 把 channel 交给 work pool 处理
		}
		// 判断 dirty 的个数
		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}
		// 大于 1/4 的话不进行睡眠，继续运行，因为这时大量的 channel 需要处理
		// TODO 这可能导致长时间 channels 得不到更新，因为如果一直处于忙碌状态的话
		// 不会执行 select 即使 channel 发生了变化也不会更新
		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

// 构建 tls 配置信息
func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

// 验证功能是否开启
func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
