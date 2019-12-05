package nsqd

// 实现 BackendQueue 接口，但是没有逻辑，把消息丢弃即可
type dummyBackendQueue struct {
	readChan chan []byte
}

func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

func (d *dummyBackendQueue) Put([]byte) error {
	return nil
}

func (d *dummyBackendQueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *dummyBackendQueue) Close() error {
	return nil
}

func (d *dummyBackendQueue) Delete() error {
	return nil
}

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}
