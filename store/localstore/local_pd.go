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

// ChangeRegionInfo used for test handling region info change.
func ChangeRegionInfo(regionId int, startKey, endKey []byte) {
	for i, region := range pd.regions {
		if region.rs.id == regionId {
			newRegionInfo := &regionInfo{
				startKey: startKey,
				endKey:   endKey,
			}
			newRegionInfo.rs = region.rs
			region.rs.startKey = startKey
			region.rs.endKey = endKey
			pd.regions[i] = newRegionInfo
			break
		}
	}
}
