package nsqd

// TODO 这种手法还是第一次见
// 对 NSQD 的封装而已，并不是 go 的 context
type context struct {
	nsqd *NSQD
}

