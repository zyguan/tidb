package localstore

type localPD struct {
	regions []*regionInfo
}

type regionInfo struct {
	startKey []byte
	endKey   []byte
	rs       *localRS
}

func (pd *localPD) GetRegionInfo() []*regionInfo {
	return pd.regions
}

func (pd *localPD) SetRegionInfo(regions []*regionInfo) {
	pd.regions = regions
}
