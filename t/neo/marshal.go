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

func (p *FinishTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.OIDList = make([]neo.Oid, l)
		for i := 0; i < l; i++ {
			a := &p.OIDList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		p.CheckedList = make([]neo.Oid, l)
		for i := 0; i < l; i++ {
			a := &p.CheckedList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerFinishTransaction) NEODecode(data []byte) (int, error) {
	p.TTID = BigEndian.Uint64(data[0:])
	p.Tid = BigEndian.Uint64(data[8:])
	return 16 /* + TODO variable part */, nil
}

func (p *NotifyTransactionFinished) NEODecode(data []byte) (int, error) {
	p.TTID = BigEndian.Uint64(data[0:])
	p.MaxTID = BigEndian.Uint64(data[8:])
	return 16 /* + TODO variable part */, nil
}

func (p *LockInformation) NEODecode(data []byte) (int, error) {
	p.Ttid = BigEndian.Uint64(data[0:])
	p.Tid = BigEndian.Uint64(data[8:])
	return 16 /* + TODO variable part */, nil
}

func (p *AnswerLockInformation) NEODecode(data []byte) (int, error) {
	p.Ttid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *InvalidateObjects) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.OidList = make([]neo.Oid, l)
		for i := 0; i < l; i++ {
			a := &p.OidList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *UnlockInformation) NEODecode(data []byte) (int, error) {
	p.TTID = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *GenerateOIDs) NEODecode(data []byte) (int, error) {
	p.NumOIDs = BigEndian.Uint32(data[0:])
	return 4 /* + TODO variable part */, nil
}

func (p *AnswerGenerateOIDs) NEODecode(data []byte) (int, error) {
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		p.OidList = make([]neo.Oid, l)
		for i := 0; i < l; i++ {
			a := &p.OidList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *StoreObject) NEODecode(data []byte) (int, error) {
	p.Oid = BigEndian.Uint64(data[0:])
	p.Serial = BigEndian.Uint64(data[8:])
	p.Compression = bool((data[16:])[0])
	p.Checksum[0] = (data[17:])[0]
	p.Checksum[1] = (data[18:])[0]
	p.Checksum[2] = (data[19:])[0]
	p.Checksum[3] = (data[20:])[0]
	p.Checksum[4] = (data[21:])[0]
	p.Checksum[5] = (data[22:])[0]
	p.Checksum[6] = (data[23:])[0]
	p.Checksum[7] = (data[24:])[0]
	p.Checksum[8] = (data[25:])[0]
	p.Checksum[9] = (data[26:])[0]
	p.Checksum[10] = (data[27:])[0]
	p.Checksum[11] = (data[28:])[0]
	p.Checksum[12] = (data[29:])[0]
	p.Checksum[13] = (data[30:])[0]
	p.Checksum[14] = (data[31:])[0]
	p.Checksum[15] = (data[32:])[0]
	p.Checksum[16] = (data[33:])[0]
	p.Checksum[17] = (data[34:])[0]
	p.Checksum[18] = (data[35:])[0]
	p.Checksum[19] = (data[36:])[0]
	{
		l := BigEndian.Uint32(data[37:])
		data = data[41:]
		p.Data = make([]byte, l)
		for i := 0; i < l; i++ {
			a := &p.Data[i]
			a = (data[0:])[0]
			data = data[1:]
		}
	}
	p.DataSerial = BigEndian.Uint64(data[0:])
	p.Tid = BigEndian.Uint64(data[8:])
	p.Unlock = bool((data[16:])[0])
	return 17 /* + TODO variable part */, nil
}

func (p *AnswerStoreObject) NEODecode(data []byte) (int, error) {
	p.Conflicting = bool((data[0:])[0])
	p.Oid = BigEndian.Uint64(data[1:])
	p.Serial = BigEndian.Uint64(data[9:])
	return 17 /* + TODO variable part */, nil
}

func (p *AbortTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *StoreTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.User = string(data[:l])
		data = data[l:]
	}
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Description = string(data[:l])
		data = data[l:]
	}
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Extension = string(data[:l])
		data = data[l:]
	}
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		p.OidList = make([]neo.Oid, l)
		for i := 0; i < l; i++ {
			a := &p.OidList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *VoteTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *GetObject) NEODecode(data []byte) (int, error) {
	p.Oid = BigEndian.Uint64(data[0:])
	p.Serial = BigEndian.Uint64(data[8:])
	p.Tid = BigEndian.Uint64(data[16:])
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerGetObject) NEODecode(data []byte) (int, error) {
	p.Oid = BigEndian.Uint64(data[0:])
	p.SerialStart = BigEndian.Uint64(data[8:])
	p.SerialEnd = BigEndian.Uint64(data[16:])
	p.Compression = bool((data[24:])[0])
	p.Checksum[0] = (data[25:])[0]
	p.Checksum[1] = (data[26:])[0]
	p.Checksum[2] = (data[27:])[0]
	p.Checksum[3] = (data[28:])[0]
	p.Checksum[4] = (data[29:])[0]
	p.Checksum[5] = (data[30:])[0]
	p.Checksum[6] = (data[31:])[0]
	p.Checksum[7] = (data[32:])[0]
	p.Checksum[8] = (data[33:])[0]
	p.Checksum[9] = (data[34:])[0]
	p.Checksum[10] = (data[35:])[0]
	p.Checksum[11] = (data[36:])[0]
	p.Checksum[12] = (data[37:])[0]
	p.Checksum[13] = (data[38:])[0]
	p.Checksum[14] = (data[39:])[0]
	p.Checksum[15] = (data[40:])[0]
	p.Checksum[16] = (data[41:])[0]
	p.Checksum[17] = (data[42:])[0]
	p.Checksum[18] = (data[43:])[0]
	p.Checksum[19] = (data[44:])[0]
	{
		l := BigEndian.Uint32(data[45:])
		data = data[49:]
		p.Data = make([]byte, l)
		for i := 0; i < l; i++ {
			a := &p.Data[i]
			a = (data[0:])[0]
			data = data[1:]
		}
	}
	p.DataSerial = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *TIDList) NEODecode(data []byte) (int, error) {
	p.First = BigEndian.Uint64(data[0:])
	p.Last = BigEndian.Uint64(data[8:])
	p.Partition = BigEndian.Uint32(data[16:])
	return 20 /* + TODO variable part */, nil
}

func (p *AnswerTIDList) NEODecode(data []byte) (int, error) {
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		p.TIDList = make([]neo.Tid, l)
		for i := 0; i < l; i++ {
			a := &p.TIDList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *TIDListFrom) NEODecode(data []byte) (int, error) {
	p.MinTID = BigEndian.Uint64(data[0:])
	p.MaxTID = BigEndian.Uint64(data[8:])
	p.Length = BigEndian.Uint32(data[16:])
	p.Partition = BigEndian.Uint32(data[20:])
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerTIDListFrom) NEODecode(data []byte) (int, error) {
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		p.TidList = make([]neo.Tid, l)
		for i := 0; i < l; i++ {
			a := &p.TidList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *TransactionInformation) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerTransactionInformation) NEODecode(data []byte) (int, error) {
	p.Tid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.User = string(data[:l])
		data = data[l:]
	}
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Description = string(data[:l])
		data = data[l:]
	}
	{
		l := BigEndian.Uint32(data[0:])
		data = data[4:]
		if len(data) < l {
			return 0, ErrDecodeOverflow
		}
		p.Extension = string(data[:l])
		data = data[l:]
	}
	p.Packed = bool((data[0:])[0])
	{
		l := BigEndian.Uint32(data[1:])
		data = data[5:]
		p.OidList = make([]neo.Oid, l)
		for i := 0; i < l; i++ {
			a := &p.OidList[i]
			a = BigEndian.Uint64(data[0:])
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *ObjectHistory) NEODecode(data []byte) (int, error) {
	p.Oid = BigEndian.Uint64(data[0:])
	p.First = BigEndian.Uint64(data[8:])
	p.Last = BigEndian.Uint64(data[16:])
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerObjectHistory) NEODecode(data []byte) (int, error) {
	p.Oid = BigEndian.Uint64(data[0:])
	{
		l := BigEndian.Uint32(data[8:])
		data = data[12:]
		p.HistoryList = make([]struct {
			Serial neo.Tid
			Size   uint32
		}, l)
		for i := 0; i < l; i++ {
			a := &p.HistoryList[i]
			a.Serial = BigEndian.Uint64(data[0:])
			a.Size = BigEndian.Uint32(data[8:])
			data = data[12:]
		}
	}
	return 0 /* + TODO variable part */, nil
}
