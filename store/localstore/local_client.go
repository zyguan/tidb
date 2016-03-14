package localstore

import "github.com/pingcap/tidb/kv"

type localClient struct {
	regionInfo []*regionInfo
}

func (c *localClient) Send(req *kv.Request) kv.ResponseIterator {
	it := c.buildRespIterator(req)
	it.run()
	return it
}

func (c *localClient) SupportRequestType(reqType, subType int64) bool {
	return false
}

func (c *localClient) buildRespIterator(req *kv.Request) *respIterator {
	it := &respIterator{
		client:      c,
		concurrency: req.Concurrency,
		taskChan:    make(chan *task, req.Concurrency),
		errChan:     make(chan error, req.Concurrency),
		respChan:    make(chan *regionResponse, req.Concurrency),
	}
	// TODO: Implement
	return it
}

func (c *localClient) updateRegionInfo() {
	c.regionInfo = pd.GetRegionInfo()
}

type localResponse struct {
	rows     []*tipb.Row
	cursor   int
	finished true
}

func (r *localResponse) Next() ([]byte, error) {
	if r.finished {
		return nil, nil
	}
	if cursor >= len(r.rows) {
		r.finished = true
		return nil, nil
	}
	d := r.rows[r.cursor]
	r.cursor++
	// convert d to bytes
	return r.rowToBytes(d), nil
}

func (r *localResponse) rowToBytes(row *tipb.Row) ([]byte, error) {
	bs, err := proto.Marshal(row)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return bs, err
}

type respIterator struct {
	client      *localClient
	reqSent     int64
	respGot     int64
	concurrency int64
	tasks       []*task
	responses   []*regionResponse
	taskChan    chan *task
	respChan    chan *regionResponse
	errChan     chan error
	finished    bool
}

type task struct {
	request *regionRequest
	region  *localRS
}

func (it *respIterator) Next() (resp []byte, err error) {
	if it.finished {
		return nil, nil
	}
	var regionResp *regionResponse
	select {
	case regionResp = <-it.respChan:
	case err = <-it.errChan:
	}
	if err != nil {
		it.Close()
		return nil, err
	}
	if len(regionResp.newStartKey) != 0 {
		it.client.updateRegionInfo()
		retryTasks := it.createRetryTasks(regionResp)
		it.tasks = append(it.tasks, retryTasks...)
	}
	if it.reqSent < len(it.tasks) {
		it.taskChan <- it.tasks[it.reqSent]
		it.reqSent++
	}
	it.respGot++
	if it.reqSent == len(it.tasks) && it.respGot == it.reqSent {
		it.Close()
	}
	resp := new(tipb.SelectResponse)
	rs, err := proto.Unmarshal(regionResp.Data, resp)
	if err != nil {
		it.Close()
		return nil, errors.Trace(err)
	}
	lr := localResponse{
		rows: rs.Rows,
	}
	return lr, nil
}

func (it *respIterator) createRetryTasks(resp *regionResponse) []*task {
	return nil
}

func (it *respIterator) Close() error {
	// Make goroutines quit.
	if it.finished {
		return nil
	}
	close(it.taskChan)
	it.finished = true
	return nil
}

func (it *respIterator) run() {
	for i := 0; i < it.concurrency; i++ {
		go func() {
			for task := range it.taskChan {
				resp, err := task.region.Handle(task.request)
				if err != nil {
					it.errChan <- err
					break
				}
				it.respChan <- resp
			}
		}()
		it.taskChan <- it.tasks[i]
	}
}
