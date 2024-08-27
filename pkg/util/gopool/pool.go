package gopool

import "time"

// Pool likes https://github.com/tiancaiamao/gp/blob/main/gp.go but without cap limit.
type Pool struct {
	tasks       chan func()
	closed      chan struct{}
	maxIdleTime time.Duration
}

// New create a new goroutine pool.
// The dur parameter controls the idle recycle behaviour. If the goroutine in the pool is idle for a while, it will be recycled.
func New(dur time.Duration) *Pool {
	return &Pool{
		tasks:       make(chan func()),
		closed:      make(chan struct{}),
		maxIdleTime: dur,
	}
}

// Run execute the function in a seperate goroutine,
func (p *Pool) Go(f func()) {
	select {
	case p.tasks <- f:
	case <-p.closed:
	default:
		go workerLoop(p, f)
	}
}

func workerLoop(p *Pool, fn func()) {
	fn()
	if p.maxIdleTime > 0 {
		// worker loop with recycle
		t := time.NewTimer(p.maxIdleTime)
		done := false
		for !done {
			select {
			case f := <-p.tasks:
				f()
				if !t.Stop() {
					<-t.C
				}
				t.Reset(p.maxIdleTime)
			case <-t.C:
				done = true
			case <-p.closed:
				done = true
			}
		}
	} else if p.maxIdleTime < 0 {
		// worker loop without recycle
		for {
			select {
			case f := <-p.tasks:
				f()
			case <-p.closed:
				return
			}
		}
	}
	// exit if p.maxIdleTime == 0
}

// Close releases the goroutines in the Pool.
// After this operation, inflight tasks may still execute until finish.
// But all the new coming tasks will be simply ignored.
func (p *Pool) Close() {
	close(p.closed)
}
