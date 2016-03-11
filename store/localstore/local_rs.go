package localstore

// local region server.
type localRS struct {
}

type regionRequest struct {
	Tp       int64
	data     []byte
	startKey []byte
	endKey   []byte
}

type regionResponse struct {
	req  *regionRequest
	err  error
	data []byte
	// If region missed some request key range, newStartKey and newEndKey is returned.
	newStartKey []byte
	newEndKey   []byte
}

func (rs *localRS) Handle(req *regionRequest) (*regionResponse, error) {
	return nil, nil
}
