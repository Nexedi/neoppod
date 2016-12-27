package neo

func (p *Address) NEODecode(data []byte) (int, error) {
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Host = string(data[:l])
		data = data[l:]
	}
	p.Port = BigEndian.Uint16(data[0:])
	return 2 /* + TODO variable part */, nil
}
func (p *NodeInfo) NEODecode(data []byte) (int, error) {
	p.NodeType = int32(BigEndian.Uint32(data[0:]))
	{
		l := BigEndian.Uint32(data[4:])
		data = data[8:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Address.Host = string(data[:l])
		data = data[l:]
	}
	p.Address.Port = BigEndian.Uint16(data[0:])
	p.UUID = int32(BigEndian.Uint32(data[2:]))
	p.NodeState = int32(BigEndian.Uint32(data[6:]))
	p.IdTimestamp = float64_NEODecode(data[10:])
	return 18 /* + TODO variable part */, nil
}
func (p *CellInfo) NEODecode(data []byte) (int, error) {
	p.UUID = int32(BigEndian.Uint32(data[0:]))
	p.CellState = int32(BigEndian.Uint32(data[4:]))
	return 8 /* + TODO variable part */, nil
}
