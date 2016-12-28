// DO NOT EDIT - AUTOGENERATED (by protogen.go)

package neo

import (
	"encoding/binary"
)

func (p *Address) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Host = string(data[:l])
		data = data[l:]
	}
	p.Port = binary.BigEndian.Uint16(data[0:])
	return 2 /* + TODO variable part */, nil
}

func (p *NodeInfo) NEODecode(data []byte) (int, error) {
	p.NodeType = NodeType(int32(binary.BigEndian.Uint32(data[0:])))
	{
		l := binary.BigEndian.Uint32(data[4:])
		data = data[8:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Address.Host = string(data[:l])
		data = data[l:]
	}
	p.Address.Port = binary.BigEndian.Uint16(data[0:])
	p.UUID = UUID(int32(binary.BigEndian.Uint32(data[2:])))
	p.NodeState = NodeState(int32(binary.BigEndian.Uint32(data[6:])))
	p.IdTimestamp = float64_NEODecode(data[10:])
	return 18 /* + TODO variable part */, nil
}

func (p *CellInfo) NEODecode(data []byte) (int, error) {
	p.UUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
	p.CellState = CellState(int32(binary.BigEndian.Uint32(data[4:])))
	return 8 /* + TODO variable part */, nil
}

func (p *RowInfo) NEODecode(data []byte) (int, error) {
	p.Offset = binary.BigEndian.Uint32(data[0:])
	{
		l := binary.BigEndian.Uint32(data[4:])
		data = data[8:]
		p.CellList = make([]CellInfo, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.CellList[i]
			(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
			(*a).CellState = CellState(int32(binary.BigEndian.Uint32(data[4:])))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *Notify) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Message = string(data[:l])
		data = data[l:]
	}
	return 0 /* + TODO variable part */, nil
}

func (p *Error) NEODecode(data []byte) (int, error) {
	p.Code = binary.BigEndian.Uint32(data[0:])
	{
		l := binary.BigEndian.Uint32(data[4:])
		data = data[8:]
		if uint32(len(data)) < l {
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
	p.ProtocolVersion = binary.BigEndian.Uint32(data[0:])
	p.NodeType = NodeType(int32(binary.BigEndian.Uint32(data[4:])))
	p.UUID = UUID(int32(binary.BigEndian.Uint32(data[8:])))
	{
		l := binary.BigEndian.Uint32(data[12:])
		data = data[16:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Address.Host = string(data[:l])
		data = data[l:]
	}
	p.Address.Port = binary.BigEndian.Uint16(data[0:])
	{
		l := binary.BigEndian.Uint32(data[2:])
		data = data[6:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Name = string(data[:l])
		data = data[l:]
	}
	p.IdTimestamp = float64_NEODecode(data[0:])
	return 8 /* + TODO variable part */, nil
}

func (p *AcceptIdentification) NEODecode(data []byte) (int, error) {
	p.NodeType = NodeType(int32(binary.BigEndian.Uint32(data[0:])))
	p.MyUUID = UUID(int32(binary.BigEndian.Uint32(data[4:])))
	p.NumPartitions = binary.BigEndian.Uint32(data[8:])
	p.NumReplicas = binary.BigEndian.Uint32(data[12:])
	p.YourUUID = UUID(int32(binary.BigEndian.Uint32(data[16:])))
	{
		l := binary.BigEndian.Uint32(data[20:])
		data = data[24:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Primary.Host = string(data[:l])
		data = data[l:]
	}
	p.Primary.Port = binary.BigEndian.Uint16(data[0:])
	{
		l := binary.BigEndian.Uint32(data[2:])
		data = data[6:]
		p.KnownMasterList = make([]struct {
			Address
			UUID UUID
		}, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.KnownMasterList[i]
			{
				l := binary.BigEndian.Uint32(data[0:])
				data = data[4:]
				if uint32(len(data)) < l {
					return 0, ErrDecodeOverflow
				}
				(*a).Address.Host = string(data[:l])
				data = data[l:]
			}
			(*a).Address.Port = binary.BigEndian.Uint16(data[0:])
			(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[2:])))
			data = data[6:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *PrimaryMaster) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerPrimary) NEODecode(data []byte) (int, error) {
	p.PrimaryUUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
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
	p.PTid = PTid(binary.BigEndian.Uint64(data[0:]))
	p.BackupTID = Tid(binary.BigEndian.Uint64(data[8:]))
	p.TruncateTID = Tid(binary.BigEndian.Uint64(data[16:]))
	return 24 /* + TODO variable part */, nil
}

func (p *LastIDs) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerLastIDs) NEODecode(data []byte) (int, error) {
	p.LastOID = Oid(binary.BigEndian.Uint64(data[0:]))
	p.LastTID = Tid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *PartitionTable) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerPartitionTable) NEODecode(data []byte) (int, error) {
	p.PTid = PTid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.RowList = make([]RowInfo, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.RowList[i]
			(*a).Offset = binary.BigEndian.Uint32(data[0:])
			{
				l := binary.BigEndian.Uint32(data[4:])
				data = data[8:]
				(*a).CellList = make([]CellInfo, l)
				for i := 0; uint32(i) < l; i++ {
					a := &(*a).CellList[i]
					(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
					(*a).CellState = CellState(int32(binary.BigEndian.Uint32(data[4:])))
					data = data[8:]
				}
			}
			data = data[0:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *NotifyPartitionTable) NEODecode(data []byte) (int, error) {
	p.PTid = PTid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.RowList = make([]RowInfo, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.RowList[i]
			(*a).Offset = binary.BigEndian.Uint32(data[0:])
			{
				l := binary.BigEndian.Uint32(data[4:])
				data = data[8:]
				(*a).CellList = make([]CellInfo, l)
				for i := 0; uint32(i) < l; i++ {
					a := &(*a).CellList[i]
					(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
					(*a).CellState = CellState(int32(binary.BigEndian.Uint32(data[4:])))
					data = data[8:]
				}
			}
			data = data[0:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *PartitionChanges) NEODecode(data []byte) (int, error) {
	p.PTid = PTid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.CellList = make([]struct {
			Offset    uint32
			UUID      UUID
			CellState CellState
		}, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.CellList[i]
			(*a).Offset = binary.BigEndian.Uint32(data[0:])
			(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[4:])))
			(*a).CellState = CellState(int32(binary.BigEndian.Uint32(data[8:])))
			data = data[12:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *StartOperation) NEODecode(data []byte) (int, error) {
	p.Backup = byte2bool((data[0:])[0])
	return 1 /* + TODO variable part */, nil
}

func (p *StopOperation) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *UnfinishedTransactions) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerUnfinishedTransactions) NEODecode(data []byte) (int, error) {
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.TidList = make([]struct{ UnfinishedTID Tid }, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.TidList[i]
			(*a).UnfinishedTID = Tid(binary.BigEndian.Uint64(data[0:]))
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
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.TidDict = make(map[Tid]Tid, l)
		m := p.TidDict
		for i := 0; uint32(i) < l; i++ {
			key := Tid(binary.BigEndian.Uint64(data[0:]))
			m[key] = Tid(binary.BigEndian.Uint64(data[8:]))
			data = data[16:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *FinalTID) NEODecode(data []byte) (int, error) {
	p.TTID = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerFinalTID) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *ValidateTransaction) NEODecode(data []byte) (int, error) {
	p.TTID = Tid(binary.BigEndian.Uint64(data[0:]))
	p.Tid = Tid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *BeginTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerBeginTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *FinishTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.OIDList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.OIDList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.CheckedList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.CheckedList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerFinishTransaction) NEODecode(data []byte) (int, error) {
	p.TTID = Tid(binary.BigEndian.Uint64(data[0:]))
	p.Tid = Tid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *NotifyTransactionFinished) NEODecode(data []byte) (int, error) {
	p.TTID = Tid(binary.BigEndian.Uint64(data[0:]))
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *LockInformation) NEODecode(data []byte) (int, error) {
	p.Ttid = Tid(binary.BigEndian.Uint64(data[0:]))
	p.Tid = Tid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *AnswerLockInformation) NEODecode(data []byte) (int, error) {
	p.Ttid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *InvalidateObjects) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.OidList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.OidList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *UnlockInformation) NEODecode(data []byte) (int, error) {
	p.TTID = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *GenerateOIDs) NEODecode(data []byte) (int, error) {
	p.NumOIDs = binary.BigEndian.Uint32(data[0:])
	return 4 /* + TODO variable part */, nil
}

func (p *AnswerGenerateOIDs) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.OidList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.OidList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *StoreObject) NEODecode(data []byte) (int, error) {
	p.Oid = Oid(binary.BigEndian.Uint64(data[0:]))
	p.Serial = Tid(binary.BigEndian.Uint64(data[8:]))
	p.Compression = byte2bool((data[16:])[0])
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
		l := binary.BigEndian.Uint32(data[37:])
		data = data[41:]
		p.Data = make([]byte, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.Data[i]
			(*a) = (data[0:])[0]
			data = data[1:]
		}
	}
	p.DataSerial = Tid(binary.BigEndian.Uint64(data[0:]))
	p.Tid = Tid(binary.BigEndian.Uint64(data[8:]))
	p.Unlock = byte2bool((data[16:])[0])
	return 17 /* + TODO variable part */, nil
}

func (p *AnswerStoreObject) NEODecode(data []byte) (int, error) {
	p.Conflicting = byte2bool((data[0:])[0])
	p.Oid = Oid(binary.BigEndian.Uint64(data[1:]))
	p.Serial = Tid(binary.BigEndian.Uint64(data[9:]))
	return 17 /* + TODO variable part */, nil
}

func (p *AbortTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *StoreTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.User = string(data[:l])
		data = data[l:]
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Description = string(data[:l])
		data = data[l:]
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Extension = string(data[:l])
		data = data[l:]
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.OidList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.OidList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *VoteTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *GetObject) NEODecode(data []byte) (int, error) {
	p.Oid = Oid(binary.BigEndian.Uint64(data[0:]))
	p.Serial = Tid(binary.BigEndian.Uint64(data[8:]))
	p.Tid = Tid(binary.BigEndian.Uint64(data[16:]))
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerGetObject) NEODecode(data []byte) (int, error) {
	p.Oid = Oid(binary.BigEndian.Uint64(data[0:]))
	p.SerialStart = Tid(binary.BigEndian.Uint64(data[8:]))
	p.SerialEnd = Tid(binary.BigEndian.Uint64(data[16:]))
	p.Compression = byte2bool((data[24:])[0])
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
		l := binary.BigEndian.Uint32(data[45:])
		data = data[49:]
		p.Data = make([]byte, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.Data[i]
			(*a) = (data[0:])[0]
			data = data[1:]
		}
	}
	p.DataSerial = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *TIDList) NEODecode(data []byte) (int, error) {
	p.First = binary.BigEndian.Uint64(data[0:])
	p.Last = binary.BigEndian.Uint64(data[8:])
	p.Partition = binary.BigEndian.Uint32(data[16:])
	return 20 /* + TODO variable part */, nil
}

func (p *AnswerTIDList) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.TIDList = make([]Tid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.TIDList[i]
			(*a) = Tid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *TIDListFrom) NEODecode(data []byte) (int, error) {
	p.MinTID = Tid(binary.BigEndian.Uint64(data[0:]))
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[8:]))
	p.Length = binary.BigEndian.Uint32(data[16:])
	p.Partition = binary.BigEndian.Uint32(data[20:])
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerTIDListFrom) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.TidList = make([]Tid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.TidList[i]
			(*a) = Tid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *TransactionInformation) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerTransactionInformation) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.User = string(data[:l])
		data = data[l:]
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Description = string(data[:l])
		data = data[l:]
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Extension = string(data[:l])
		data = data[l:]
	}
	p.Packed = byte2bool((data[0:])[0])
	{
		l := binary.BigEndian.Uint32(data[1:])
		data = data[5:]
		p.OidList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.OidList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *ObjectHistory) NEODecode(data []byte) (int, error) {
	p.Oid = Oid(binary.BigEndian.Uint64(data[0:]))
	p.First = binary.BigEndian.Uint64(data[8:])
	p.Last = binary.BigEndian.Uint64(data[16:])
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerObjectHistory) NEODecode(data []byte) (int, error) {
	p.Oid = Oid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.HistoryList = make([]struct {
			Serial Tid
			Size   uint32
		}, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.HistoryList[i]
			(*a).Serial = Tid(binary.BigEndian.Uint64(data[0:]))
			(*a).Size = binary.BigEndian.Uint32(data[8:])
			data = data[12:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *PartitionList) NEODecode(data []byte) (int, error) {
	p.MinOffset = binary.BigEndian.Uint32(data[0:])
	p.MaxOffset = binary.BigEndian.Uint32(data[4:])
	p.UUID = UUID(int32(binary.BigEndian.Uint32(data[8:])))
	return 12 /* + TODO variable part */, nil
}

func (p *AnswerPartitionList) NEODecode(data []byte) (int, error) {
	p.PTid = PTid(binary.BigEndian.Uint64(data[0:]))
	{
		l := binary.BigEndian.Uint32(data[8:])
		data = data[12:]
		p.RowList = make([]RowInfo, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.RowList[i]
			(*a).Offset = binary.BigEndian.Uint32(data[0:])
			{
				l := binary.BigEndian.Uint32(data[4:])
				data = data[8:]
				(*a).CellList = make([]CellInfo, l)
				for i := 0; uint32(i) < l; i++ {
					a := &(*a).CellList[i]
					(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
					(*a).CellState = CellState(int32(binary.BigEndian.Uint32(data[4:])))
					data = data[8:]
				}
			}
			data = data[0:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *X_NodeList) NEODecode(data []byte) (int, error) {
	p.NodeType = NodeType(int32(binary.BigEndian.Uint32(data[0:])))
	return 4 /* + TODO variable part */, nil
}

func (p *AnswerNodeList) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.NodeList = make([]NodeInfo, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.NodeList[i]
			(*a).NodeType = NodeType(int32(binary.BigEndian.Uint32(data[0:])))
			{
				l := binary.BigEndian.Uint32(data[4:])
				data = data[8:]
				if uint32(len(data)) < l {
					return 0, ErrDecodeOverflow
				}
				(*a).Address.Host = string(data[:l])
				data = data[l:]
			}
			(*a).Address.Port = binary.BigEndian.Uint16(data[0:])
			(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[2:])))
			(*a).NodeState = NodeState(int32(binary.BigEndian.Uint32(data[6:])))
			(*a).IdTimestamp = float64_NEODecode(data[10:])
			data = data[18:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *SetNodeState) NEODecode(data []byte) (int, error) {
	p.UUID = UUID(int32(binary.BigEndian.Uint32(data[0:])))
	p.NodeState = NodeState(int32(binary.BigEndian.Uint32(data[4:])))
	return 8 /* + TODO variable part */, nil
}

func (p *AddPendingNodes) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.UUIDList = make([]UUID, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.UUIDList[i]
			(*a) = UUID(int32(binary.BigEndian.Uint32(data[0:])))
			data = data[4:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *TweakPartitionTable) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.UUIDList = make([]UUID, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.UUIDList[i]
			(*a) = UUID(int32(binary.BigEndian.Uint32(data[0:])))
			data = data[4:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *NotifyNodeInformation) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.NodeList = make([]NodeInfo, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.NodeList[i]
			(*a).NodeType = NodeType(int32(binary.BigEndian.Uint32(data[0:])))
			{
				l := binary.BigEndian.Uint32(data[4:])
				data = data[8:]
				if uint32(len(data)) < l {
					return 0, ErrDecodeOverflow
				}
				(*a).Address.Host = string(data[:l])
				data = data[l:]
			}
			(*a).Address.Port = binary.BigEndian.Uint16(data[0:])
			(*a).UUID = UUID(int32(binary.BigEndian.Uint32(data[2:])))
			(*a).NodeState = NodeState(int32(binary.BigEndian.Uint32(data[6:])))
			(*a).IdTimestamp = float64_NEODecode(data[10:])
			data = data[18:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *NodeInformation) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *SetClusterState) NEODecode(data []byte) (int, error) {
	p.State = ClusterState(int32(binary.BigEndian.Uint32(data[0:])))
	return 4 /* + TODO variable part */, nil
}

func (p *ClusterInformation) NEODecode(data []byte) (int, error) {
	p.State = ClusterState(int32(binary.BigEndian.Uint32(data[0:])))
	return 4 /* + TODO variable part */, nil
}

func (p *X_ClusterState) NEODecode(data []byte) (int, error) {
	p.State = ClusterState(int32(binary.BigEndian.Uint32(data[0:])))
	return 4 /* + TODO variable part */, nil
}

func (p *ObjectUndoSerial) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	p.LTID = Tid(binary.BigEndian.Uint64(data[8:]))
	p.UndoneTID = Tid(binary.BigEndian.Uint64(data[16:]))
	{
		l := binary.BigEndian.Uint32(data[24:])
		data = data[28:]
		p.OidList = make([]Oid, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.OidList[i]
			(*a) = Oid(binary.BigEndian.Uint64(data[0:]))
			data = data[8:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerObjectUndoSerial) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.ObjectTIDDict = make(map[Oid]struct {
			CurrentSerial Tid
			UndoSerial    Tid
			IsCurrent     bool
		}, l)
		m := p.ObjectTIDDict
		for i := 0; uint32(i) < l; i++ {
			key := Oid(binary.BigEndian.Uint64(data[0:]))
			var v struct {
				CurrentSerial Tid
				UndoSerial    Tid
				IsCurrent     bool
			}
			v.CurrentSerial = Tid(binary.BigEndian.Uint64(data[8:]))
			v.UndoSerial = Tid(binary.BigEndian.Uint64(data[16:]))
			v.IsCurrent = byte2bool((data[24:])[0])
			m[key] = v
			data = data[25:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *HasLock) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	p.Oid = Oid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *AnswerHasLock) NEODecode(data []byte) (int, error) {
	p.Oid = Oid(binary.BigEndian.Uint64(data[0:]))
	p.LockState = LockState(int32(binary.BigEndian.Uint32(data[8:])))
	return 12 /* + TODO variable part */, nil
}

func (p *CheckCurrentSerial) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	p.Serial = Tid(binary.BigEndian.Uint64(data[8:]))
	p.Oid = Oid(binary.BigEndian.Uint64(data[16:]))
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerCheckCurrentSerial) NEODecode(data []byte) (int, error) {
	p.Conflicting = byte2bool((data[0:])[0])
	p.Oid = Oid(binary.BigEndian.Uint64(data[1:]))
	p.Serial = Tid(binary.BigEndian.Uint64(data[9:]))
	return 17 /* + TODO variable part */, nil
}

func (p *Pack) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *AnswerPack) NEODecode(data []byte) (int, error) {
	p.Status = byte2bool((data[0:])[0])
	return 1 /* + TODO variable part */, nil
}

func (p *CheckReplicas) NEODecode(data []byte) (int, error) {
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		p.PartitionDict = make(map[uint32]UUID, l)
		m := p.PartitionDict
		for i := 0; uint32(i) < l; i++ {
			key := binary.BigEndian.Uint32(data[0:])
			m[key] = UUID(int32(binary.BigEndian.Uint32(data[4:])))
			data = data[8:]
		}
	}
	p.MinTID = Tid(binary.BigEndian.Uint64(data[0:]))
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[8:]))
	return 16 /* + TODO variable part */, nil
}

func (p *CheckPartition) NEODecode(data []byte) (int, error) {
	p.Partition = binary.BigEndian.Uint32(data[0:])
	{
		l := binary.BigEndian.Uint32(data[4:])
		data = data[8:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Source.UpstreamName = string(data[:l])
		data = data[l:]
	}
	{
		l := binary.BigEndian.Uint32(data[0:])
		data = data[4:]
		if uint32(len(data)) < l {
			return 0, ErrDecodeOverflow
		}
		p.Source.Address.Host = string(data[:l])
		data = data[l:]
	}
	p.Source.Address.Port = binary.BigEndian.Uint16(data[0:])
	p.MinTID = Tid(binary.BigEndian.Uint64(data[2:]))
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[10:]))
	return 18 /* + TODO variable part */, nil
}

func (p *CheckTIDRange) NEODecode(data []byte) (int, error) {
	p.Partition = binary.BigEndian.Uint32(data[0:])
	p.Length = binary.BigEndian.Uint32(data[4:])
	p.MinTID = Tid(binary.BigEndian.Uint64(data[8:]))
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[16:]))
	return 24 /* + TODO variable part */, nil
}

func (p *AnswerCheckTIDRange) NEODecode(data []byte) (int, error) {
	p.Count = binary.BigEndian.Uint32(data[0:])
	p.Checksum[0] = (data[4:])[0]
	p.Checksum[1] = (data[5:])[0]
	p.Checksum[2] = (data[6:])[0]
	p.Checksum[3] = (data[7:])[0]
	p.Checksum[4] = (data[8:])[0]
	p.Checksum[5] = (data[9:])[0]
	p.Checksum[6] = (data[10:])[0]
	p.Checksum[7] = (data[11:])[0]
	p.Checksum[8] = (data[12:])[0]
	p.Checksum[9] = (data[13:])[0]
	p.Checksum[10] = (data[14:])[0]
	p.Checksum[11] = (data[15:])[0]
	p.Checksum[12] = (data[16:])[0]
	p.Checksum[13] = (data[17:])[0]
	p.Checksum[14] = (data[18:])[0]
	p.Checksum[15] = (data[19:])[0]
	p.Checksum[16] = (data[20:])[0]
	p.Checksum[17] = (data[21:])[0]
	p.Checksum[18] = (data[22:])[0]
	p.Checksum[19] = (data[23:])[0]
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[24:]))
	return 32 /* + TODO variable part */, nil
}

func (p *CheckSerialRange) NEODecode(data []byte) (int, error) {
	p.Partition = binary.BigEndian.Uint32(data[0:])
	p.Length = binary.BigEndian.Uint32(data[4:])
	p.MinTID = Tid(binary.BigEndian.Uint64(data[8:]))
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[16:]))
	p.MinOID = Oid(binary.BigEndian.Uint64(data[24:]))
	return 32 /* + TODO variable part */, nil
}

func (p *AnswerCheckSerialRange) NEODecode(data []byte) (int, error) {
	p.Count = binary.BigEndian.Uint32(data[0:])
	p.TidChecksum[0] = (data[4:])[0]
	p.TidChecksum[1] = (data[5:])[0]
	p.TidChecksum[2] = (data[6:])[0]
	p.TidChecksum[3] = (data[7:])[0]
	p.TidChecksum[4] = (data[8:])[0]
	p.TidChecksum[5] = (data[9:])[0]
	p.TidChecksum[6] = (data[10:])[0]
	p.TidChecksum[7] = (data[11:])[0]
	p.TidChecksum[8] = (data[12:])[0]
	p.TidChecksum[9] = (data[13:])[0]
	p.TidChecksum[10] = (data[14:])[0]
	p.TidChecksum[11] = (data[15:])[0]
	p.TidChecksum[12] = (data[16:])[0]
	p.TidChecksum[13] = (data[17:])[0]
	p.TidChecksum[14] = (data[18:])[0]
	p.TidChecksum[15] = (data[19:])[0]
	p.TidChecksum[16] = (data[20:])[0]
	p.TidChecksum[17] = (data[21:])[0]
	p.TidChecksum[18] = (data[22:])[0]
	p.TidChecksum[19] = (data[23:])[0]
	p.MaxTID = Tid(binary.BigEndian.Uint64(data[24:]))
	p.OidChecksum[0] = (data[32:])[0]
	p.OidChecksum[1] = (data[33:])[0]
	p.OidChecksum[2] = (data[34:])[0]
	p.OidChecksum[3] = (data[35:])[0]
	p.OidChecksum[4] = (data[36:])[0]
	p.OidChecksum[5] = (data[37:])[0]
	p.OidChecksum[6] = (data[38:])[0]
	p.OidChecksum[7] = (data[39:])[0]
	p.OidChecksum[8] = (data[40:])[0]
	p.OidChecksum[9] = (data[41:])[0]
	p.OidChecksum[10] = (data[42:])[0]
	p.OidChecksum[11] = (data[43:])[0]
	p.OidChecksum[12] = (data[44:])[0]
	p.OidChecksum[13] = (data[45:])[0]
	p.OidChecksum[14] = (data[46:])[0]
	p.OidChecksum[15] = (data[47:])[0]
	p.OidChecksum[16] = (data[48:])[0]
	p.OidChecksum[17] = (data[49:])[0]
	p.OidChecksum[18] = (data[50:])[0]
	p.OidChecksum[19] = (data[51:])[0]
	p.MaxOID = Oid(binary.BigEndian.Uint64(data[52:]))
	return 60 /* + TODO variable part */, nil
}

func (p *PartitionCorrupted) NEODecode(data []byte) (int, error) {
	p.Partition = binary.BigEndian.Uint32(data[0:])
	{
		l := binary.BigEndian.Uint32(data[4:])
		data = data[8:]
		p.CellList = make([]UUID, l)
		for i := 0; uint32(i) < l; i++ {
			a := &p.CellList[i]
			(*a) = UUID(int32(binary.BigEndian.Uint32(data[0:])))
			data = data[4:]
		}
	}
	return 0 /* + TODO variable part */, nil
}

func (p *LastTransaction) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}

func (p *AnswerLastTransaction) NEODecode(data []byte) (int, error) {
	p.Tid = Tid(binary.BigEndian.Uint64(data[0:]))
	return 8 /* + TODO variable part */, nil
}

func (p *NotifyReady) NEODecode(data []byte) (int, error) {
	return 0 /* + TODO variable part */, nil
}
