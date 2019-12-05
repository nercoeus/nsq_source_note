package nsqd

// 优先队列,用来存储已经进行投递但是还没有确定的消息
type inFlightPqueue []*Message

// 创建优先队列,大小默认为 1000
func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

// 交换优先队列中的两个节点
func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// 向队列中添加元素
func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	// 扩容,容量*2
	if n+1 > c {
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	// ⭐
	*pq = (*pq)[0 : n+1]
	x.index = n
	// 添加到末尾
	(*pq)[n] = x
	// 并对这个节点进行移动
	pq.up(n)
}

// 删除一个节点
func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	// 把节点移动到末尾
	pq.Swap(0, n-1)
	// 将头结点下沉到合适的位置
	pq.down(0, n-1)
	// 缩容,小于 1/2,并且长度大于 25 才会进行缩容操作
	if n < (c/2) && c > 25 {
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// 移除第 i 个节点
func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	if n-1 != i {
		// 不是末尾节点,需要交换并将节点进行下沉和上移
		pq.Swap(i, n-1)
		pq.down(i, n-1)
		pq.up(i)
	}
	// 移出最后一个节点
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// 判断头结点需不需要取出,需要的话就 pop
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	if x.pri > max {
		return nil, x.pri - max
	}
	pq.Pop()

	return x, 0
}

// 节点上移
func (pq *inFlightPqueue) up(j int) {
	for {
		// 父节点
		i := (j - 1) / 2 // parent
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		j = i
	}
}

// 节点下沉
func (pq *inFlightPqueue) down(i, n int) {
	for {
		// 子节点
		j1 := 2*i + 1
		// 没有子节点
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		// 找到需要交换的节点(左儿子 or 右儿子)
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // = 2*i + 2  // right child
		}
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
