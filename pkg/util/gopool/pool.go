package gopool

import "time"

type Pool interface {
	Go(func())
	Close()
}

// pool likes https://github.com/tiancaiamao/gp/blob/main/gp.go but without cap limit.
type pool struct {
	tasks       chan func()
	closed      chan struct{}
	maxIdleTime time.Duration
}

// New create a new goroutine pool.
// The dur parameter controls the idle recycle behaviour. If the goroutine in the pool is idle for a while, it will be recycled.
func New(dur time.Duration) Pool {
	return &pool{
		tasks:       make(chan func()),
		closed:      make(chan struct{}),
		maxIdleTime: dur,
	}
}

// Run execute the function in a seperate goroutine.
func (p *pool) Go(f func()) {
	select {
	case p.tasks <- f:
	case <-p.closed:
		go f()
	default:
		go execloop(p, f)
	}
}

// Close releases the worker goroutines in the Pool.
func (p *pool) Close() {
	close(p.closed)
}

func execloop(p *pool, f func()) {
	f()
	if p.maxIdleTime > 0 {
		// loop with recycle
		t := time.NewTimer(p.maxIdleTime)
		for {
			select {
			case f := <-p.tasks:
				f()
				if !t.Stop() {
					<-t.C
				}
				t.Reset(p.maxIdleTime)
			case <-t.C:
				return
			case <-p.closed:
				return
			}
		}
	} else if p.maxIdleTime < 0 {
		// loop without recycle
		for {
			select {
			case f := <-p.tasks:
				f()
			case <-p.closed:
				return
			}
		}
	}
	// no loop
}
