package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/nsq/internal/auth"
)

const defaultBufferSize = 16 * 1024

// lookupPeer 的状态
const (
	stateInit = iota   // 初始化
	stateDisconnected  // 断开连接
	stateConnected     // 已经连接
	stateSubscribed    // 订阅 channel 中
	stateClosing       // 关闭连接
)

type identifyDataV2 struct {
	ClientID            string `json:"client_id"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

type identifyEvent struct {
	OutputBufferTimeout time.Duration
	HeartbeatInterval   time.Duration
	SampleRate          int32
	MsgTimeout          time.Duration
}

// nsqd 中对客户端的代理，用来和客户端通信，由 protocal_v2 管理
type clientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount    int64  // 已经确认的消息号
	InFlightCount int64  // 该 client 上待确认消息数
	MessageCount  uint64 // 接收的消息总数
	FinishCount   uint64 // 该 client 处理完并确认的消息数
	RequeueCount  uint64 // 重新推送的消息数

	pubCounts map[string]uint64 // 发送给所有 topic 的消息数

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64    // nsqd 给 client 的 ID
	ctx       *context // 上下玩环境，存储了 nsqd 的指针
	UserAgent string

	// original connection
	net.Conn // 原始 TCP 连接

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader // 读操作接口，提供 16k 的缓存空间
	Writer *bufio.Writer // 写操作接口，提供 16k 的缓存空间

	OutputBufferSize    int           // 缓冲区大小，默认 5MB
	OutputBufferTimeout time.Duration // 刷新推送缓冲区时间

	HeartbeatInterval time.Duration // 心跳检测时间间隔

	MsgTimeout time.Duration // 消息确认超时时间

	State          int32 // client 状态，详细见上面
	ConnectTime    time.Time
	Channel        *Channel // 订阅的 channel
	ReadyStateChan chan int // 通知 protocol_v2 中进行消息循环并刷新缓冲区
	ExitChan       chan int // 退出通知通道

	ClientID string // 使用的是客户端地址
	Hostname string // 客户端名称

	SampleRate int32

	IdentifyEventChan chan identifyEvent
	SubEventChan      chan *Channel // 在 IOLoop 中通知 messagePump 订阅 channel

	TLS     int32
	Snappy  int32
	Deflate int32

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	lenSlice []byte

	AuthSecret string
	AuthState  *auth.State
}

// 创建一个 V2 的客户端，用来管理和 channel 之间的消息接收
// 先添加到 nsd 中,再用 SUB 命令订阅特定 channel
func newClientV2(id int64, conn net.Conn, ctx *context) *clientV2 {
	var identifier string
	// 为每个连接生成一个特殊的标识符，用作客户端 ID
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}
	// 创建一个客户端结构体
	c := &clientV2{
		ID:  id, // ID，在 nsqd 使用这个 ID
		ctx: ctx,

		Conn: conn, // TCP 连接

		Reader: bufio.NewReaderSize(conn, defaultBufferSize), // 默认读写缓冲区大小 16kB
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,                      // 输出缓冲区
		OutputBufferTimeout: ctx.nsqd.getOpts().OutputBufferTimeout, // 刷新输出缓冲区时间间隔

		MsgTimeout: ctx.nsqd.getOpts().MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(), // 连接建立时间
		State:          stateInit,  // 初始状态

		ClientID: identifier, // 客户端标识，作为状态存储
		Hostname: identifier, // 客户端标识

		SubEventChan:      make(chan *Channel, 1), // 订阅通知通道
		IdentifyEventChan: make(chan identifyEvent, 1),

		// heartbeats are client configurable but default to 30s
		// 心跳间隔默认 30 s
		HeartbeatInterval: ctx.nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64), // 推送消息计数器
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

// 客户端地址
func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

func (c *clientV2) Identify(data identifyDataV2) error {
	c.ctx.nsqd.logf(LOG_INFO, "[%s] IDENTIFY: %+v", c, data)

	c.metaLock.Lock()
	c.ClientID = data.ClientID
	c.Hostname = data.Hostname
	c.UserAgent = data.UserAgent
	c.metaLock.Unlock()

	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	err = c.SetOutputBuffer(data.OutputBufferSize, data.OutputBufferTimeout)
	if err != nil {
		return err
	}

	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	ie := identifyEvent{
		OutputBufferTimeout: c.OutputBufferTimeout,
		HeartbeatInterval:   c.HeartbeatInterval,
		SampleRate:          c.SampleRate,
		MsgTimeout:          c.MsgTimeout,
	}

	// update the client's message pump
	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	return nil
}

// 返回客户端状态
func (c *clientV2) Stats() ClientStats {
	c.metaLock.RLock()
	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	pubCounts := make([]PubCount, 0, len(c.pubCounts))
	for topic, count := range c.pubCounts {
		pubCounts = append(pubCounts, PubCount{
			Topic: topic,
			Count: count,
		})
	}
	c.metaLock.RUnlock()
	stats := ClientStats{
		Version:         "V2",
		RemoteAddress:   c.RemoteAddr().String(),
		ClientID:        clientID,
		Hostname:        hostname,
		UserAgent:       userAgent,
		State:           atomic.LoadInt32(&c.State),
		ReadyCount:      atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:   atomic.LoadInt64(&c.InFlightCount),
		MessageCount:    atomic.LoadUint64(&c.MessageCount),
		FinishCount:     atomic.LoadUint64(&c.FinishCount),
		RequeueCount:    atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:     c.ConnectTime.Unix(),
		SampleRate:      atomic.LoadInt32(&c.SampleRate),
		TLS:             atomic.LoadInt32(&c.TLS) == 1,
		Deflate:         atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:          atomic.LoadInt32(&c.Snappy) == 1,
		Authed:          c.HasAuthorizations(),
		AuthIdentity:    identity,
		AuthIdentityURL: identityURL,
		PubCounts:       pubCounts,
	}
	if stats.TLS {
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}
	return stats
}

// 判断该 client 给 topic 推送过消息吗，是不是一个生产者
func (c *clientV2) IsProducer() bool {
	c.metaLock.RLock()
	retval := len(c.pubCounts) > 0
	c.metaLock.RUnlock()
	return retval
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}

func (p *prettyConnectionState) GetCipherSuite() string {
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionSSL30:
		return "SSL30"
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

// client 是否等待好从 channel 接收消息
func (c *clientV2) IsReadyForMessages() bool {
	if c.Channel.IsPaused() {
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	c.ctx.nsqd.logf(LOG_DEBUG, "[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}
	// 所有消息均返回确认了即可接收新消息
	return true
}

// 设置上一条消息好
func (c *clientV2) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *clientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

// 完成一条消息，处理消息计数
func (c *clientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// 清空待确认消息
func (c *clientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

// 发送给 client 一条消息
func (c *clientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

// 向 topic 推送 count 条消息
func (c *clientV2) PublishedMessage(topic string, count uint64) {
	c.metaLock.Lock()
	c.pubCounts[topic] += count
	c.metaLock.Unlock()
}

// 消息超时
func (c *clientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// 重新推送一条消息，客户端记录一下数量
func (c *clientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// 关闭 client 接收消息能力
func (c *clientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, stateClosing)
}

func (c *clientV2) Pause() {
	c.tryUpdateReadyState()
}

func (c *clientV2) UnPause() {
	c.tryUpdateReadyState()
}

// 设置心跳间隔，范围有限制
func (c *clientV2) SetHeartbeatInterval(desiredInterval int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredInterval == -1:
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.ctx.nsqd.getOpts().MaxHeartbeatInterval/time.Millisecond):
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	return nil
}

// 设置推送缓存空间和刷新缓冲区时间间隔
func (c *clientV2) SetOutputBuffer(desiredSize int, desiredTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	// 设置刷新时间间隔
	switch {
	case desiredTimeout == -1:
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// do nothing (use default)
	case true &&
		desiredTimeout >= int(c.ctx.nsqd.getOpts().MinOutputBufferTimeout/time.Millisecond) &&
		desiredTimeout <= int(c.ctx.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):

		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}
	// 设置待推送消息缓存空间
	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		c.OutputBufferSize = 1
		c.OutputBufferTimeout = 0
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(c.ctx.nsqd.getOpts().MaxOutputBufferSize):
		c.OutputBufferSize = desiredSize
	default:
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}
	// 先把缓存数据推送出去再进行重新分配
	if desiredSize != 0 {
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, c.OutputBufferSize)
	}

	return nil
}

// 设置采样率
func (c *clientV2) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

// 设置接收消息回应过期时间：待确认超时时间
func (c *clientV2) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
		// do nothing (use default)
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.ctx.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

func (c *clientV2) UpgradeTLS() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	tlsConn := tls.Server(c.Conn, c.ctx.nsqd.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, defaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

func (c *clientV2) UpgradeDeflate(level int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), defaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

func (c *clientV2) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappy.NewReader(conn), defaultBufferSize)
	c.Writer = bufio.NewWriterSize(snappy.NewWriter(conn), c.OutputBufferSize)

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

// 刷新缓冲区中待发送到客户端数据
func (c *clientV2) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

func (c *clientV2) QueryAuthd() error {
	remoteIP, _, err := net.SplitHostPort(c.String())
	if err != nil {
		return err
	}

	tlsEnabled := atomic.LoadInt32(&c.TLS) == 1
	commonName := ""
	if tlsEnabled {
		tlsConnState := c.tlsConn.ConnectionState()
		if len(tlsConnState.PeerCertificates) > 0 {
			commonName = tlsConnState.PeerCertificates[0].Subject.CommonName
		}
	}

	authState, err := auth.QueryAnyAuthd(c.ctx.nsqd.getOpts().AuthHTTPAddresses,
		remoteIP, tlsEnabled, commonName, c.AuthSecret,
		c.ctx.nsqd.getOpts().HTTPClientConnectTimeout,
		c.ctx.nsqd.getOpts().HTTPClientRequestTimeout)
	if err != nil {
		return err
	}
	c.AuthState = authState
	return nil
}

func (c *clientV2) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

func (c *clientV2) IsAuthorized(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

func (c *clientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
