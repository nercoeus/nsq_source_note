package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

// 创建 lookuppeer 的连接回调函数
func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	// 。。。
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		// 版本号
		ci["version"] = version.Binary
		// TCP port
		ci["tcp_port"] = n.RealTCPAddr().Port
		// HTTP port
		ci["http_port"] = n.RealHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		// lookupd 执行 Identify 指令
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			// 识别失败
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		} else {
			// 把回复的消息内容放到 lookupd 的 Info 中
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
				lp.Close()
				return
			} else {
				n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
				if lp.Info.BroadcastAddress == "" {
					n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
				}
			}
		}

		// build all the commands first so we exit the lock(s) as fast as possible
		// 一次性把所有 topic 和 channel 注册进 lookupd 要执行的指令，低频操作，挺费的
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()
		// 依次注册到 lookupd 中即可
		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

// 由 nsqd 子协程运行的方法
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	connect := true
	// 服务器名字
	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	// 15秒执行一次
	ticker := time.Tick(15 * time.Second)
	for {
		if connect {
			// 连接所有 lookupd addr
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				// 创建 lookupPeer
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				// 开始这个连接
				lookupPeer.Command(nil) // start the connection
				// 记录下来
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			// 记录到 nsqd 中
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}
		// 连接完毕，运行监听 nsqd 即可
		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			// 发送心跳，听取 lookupd 进程的响应
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				// Ping 指令
				cmd := nsq.Ping()
				// 执行 Ping 指令
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string
			// 判断是 topic 还是 channel 发生了改变
			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				// 封装对应的指令
				if channel.Exiting() == true {
					// channel 不用了，就注销掉
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					// 新 channel
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				// 封装对应的指令
				if topic.Exiting() == true {
					// topic 不用就注销掉
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					// 新 topic
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				// 对所有 lookupd 均发送指令
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan:
			// lookupd 配置地址更新
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					// 该连接没有变动
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				// 该链接变动了
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			// 新的 lookupPeers
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			// 将 connect 设置为 true，就会在上面重新连接新的 lookupd，并更新 nsqd 中的 lookupPeer
			connect = true
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

// 判断切片中是否存在某个对象
func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

// 返回所有 lookupd 地址
func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	// 遍历一下，记录下来即可
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		// IP + port 组成地址
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
