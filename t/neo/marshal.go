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

func (p *RowInfo) NEODecode(data []byte) (int, error) {
	p.Offset = BigEndian.Uint32(data[0:])
	{
		l := BigEndian.Uint32(data[4:])
		data = data[8:]
		p.CellList = make([]neo.CellInfo, l)
		for i := 0; i < l; i++ {
			a := &p.CellList[i]
			a.UUID = int32(BigEndian.Uint32(data[0:]))
			a.CellState = int32(BigEndian.Uint32(data[4:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *Notify) NEODecode(data []byte) (int, error) {
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Message = string(data[:l])
		data = data[l:]
	}
	return 0 /* + TODO variable part */, nil
}

func (p *Error) NEODecode(data []byte) (int, error) {
	p.Code = BigEndian.Uint32(data[0:])
	{
		l := BigEndian.Uint32(data[4:])
		data = data[8:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Message = string(data[:l])
		data = data[l:]
	}
	return 0 /* + TODO variable part */, nil
}

func (p *Ping) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *CloseClient) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *RequestIdentification) NEODecode(data []byte) (int, error) {
	p.ProtocolVersion = BigEndian.Uint32(data[0:])
	p.NodeType = int32(BigEndian.Uint32(data[4:]))
	p.UUID = int32(BigEndian.Uint32(data[8:]))
	{
		l := BigEndian.Uint32(data[12:])
		data = data[16:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Address.Host = string(data[:l])
		data = data[l:]
	}
	p.Address.Port = BigEndian.Uint16(data[0:])
	{
		l := BigEndian.Uint32(data[2:])
		data = data[6:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Name = string(data[:l])
		data = data[l:]
	}
	p.IdTimestamp = float64_NEODecode(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *AcceptIdentification) NEODecode(data []byte) (int, error) {
	p.NodeType = int32(BigEndian.Uint32(data[0:]))
	p.MyUUID = int32(BigEndian.Uint32(data[4:]))
	p.NumPartitions = BigEndian.Uint32(data[8:])
	p.NumReplicas = BigEndian.Uint32(data[12:])
	p.YourUUID = int32(BigEndian.Uint32(data[16:]))
	{
		l := BigEndian.Uint32(data[20:])
		data = data[24:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Primary.Host = string(data[:l])
		data = data[l:]
	}
	p.Primary.Port = BigEndian.Uint16(data[0:])
	{
		l := BigEndian.Uint32(data[2:])
		data = data[6:]
		p.KnownMasterList = make([]struct {
			neo.Address
			UUID neo.UUID
		}, l)
		for i := 0; i < l; i++ {
			a := &p.KnownMasterList[i]
			{
				l := BigEndian.Uint32(data[0:])
				data = data[4:]
				if len(data) < l {
					return 0, ErrDecodeOverflow
				}
				a.Address.Host = string(data[:l])
				data = data[l:]
			}
			a.Address.Port = BigEndian.Uint16(data[0:])
			a.UUID = int32(BigEndian.Uint32(data[2:]))
			data = data[6:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *PrimaryMaster) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerPrimary) NEODecode(data []byte) (int, error) {
	p.PrimaryUUID = int32(BigEndian.Uint32(data[0:]))
	return 4 /* + TODO variable part */, nil
}

func (p *AnnouncePrimary) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *ReelectPrimary) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *Recovery) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerRecovery) NEODecode(data []byte) (int, error) {
	p.PTid = BigEndian.Uint64(data[0:])
	p.BackupTID = BigEndian.Uint64(data[8:])
	p.TruncateTID = BigEndian.Uint64(data[16:])
	return 24 /* + TODO variable part */, nil
}

func (p *LastIDs) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerLastIDs) NEODecode(data []byte) (int, error) {
	p.LastOID = BigEndian.Uint64(data[0:])
	p.LastTID = BigEndian.Uint64(data[8:])
	return 16 /* + TODO variable part */, nil
}

func (p *PartitionTable) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerPartitionTable) NEODecode(data []byte) (int, error) {
	p.PTid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.RowList = make([]neo.RowInfo, l)
		for i := 0; i < l; i++ {
			a := &p.RowList[i]
			a.Offset = BigEndian.Uint32(data[0:])
			{
				l := BigEndian.Uint32(data[4:])
				data = data[8:]
				a.CellList = make([]neo.CellInfo, l)
				for i := 0; i < l; i++ {
					a := &a.CellList[i]
					a.UUID = int32(BigEndian.Uint32(data[0:]))
					a.CellState = int32(BigEndian.Uint32(data[4:]))
					data = data[8:]
				}
			}
			data = data[0:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *NotifyPartitionTable) NEODecode(data []byte) (int, error) {
	p.PTid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.RowList = make([]neo.RowInfo, l)
		for i := 0; i < l; i++ {
			a := &p.RowList[i]
			a.Offset = BigEndian.Uint32(data[0:])
			{
				l := BigEndian.Uint32(data[4:])
				data = data[8:]
				a.CellList = make([]neo.CellInfo, l)
				for i := 0; i < l; i++ {
					a := &a.CellList[i]
					a.UUID = int32(BigEndian.Uint32(data[0:]))
					a.CellState = int32(BigEndian.Uint32(data[4:]))
					data = data[8:]
				}
			}
			data = data[0:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *PartitionChanges) NEODecode(data []byte) (int, error) {
	p.PTid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.CellList = make([]struct {
			Offset    uint32
			UUID      neo.UUID
			CellState neo.CellState
		}, l)
		for i := 0; i < l; i++ {
			a := &p.CellList[i]
			a.Offset = BigEndian.Uint32(data[0:])
			a.UUID = int32(BigEndian.Uint32(data[4:]))
			a.CellState = int32(BigEndian.Uint32(data[8:]))
			data = data[12:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *StartOperation) NEODecode(data []byte) (int, error) {
	p.Backup = bool((data[0:])[0])
	return 1 /* + TODO variable part */, nil
}

func (p *StopOperation) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *UnfinishedTransactions) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerUnfinishedTransactions) NEODecode(data []byte) (int, error) {
	p.MaxTID = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.TidList = make([]struct{ UnfinishedTID neo.Tid }, l)
		for i := 0; i < l; i++ {
			a := &p.TidList[i]
			a.UnfinishedTID = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *LockedTransactions) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerLockedTransactions) NEODecode(data []byte) (int, error) {
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		p.TidDict = make(map[neo.Tid]neo.Tid, l)
		m := p.TidDict
		for i := 0; i < l; i++ {
			key = BigEndian.Uint64(data[0:])
			m[key] = BigEndian.Uint64(data[8:])
			data = data[16:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *FinalTID) NEODecode(data []byte) (int, error) {
	p.TTID = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerFinalTID) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *ValidateTransaction) NEODecode(data []byte) (int, error) {
	p.TTID = BigEndian.Uint64(data[0:])
	p.Tid = BigEndian.Uint64(data[8:])
	return 16 /* + TODO variable part */, nil
}

func (p *BeginTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerBeginTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}
