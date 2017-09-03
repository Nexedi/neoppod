// Code generated by ./py/pyneo-gen-testdata; DO NOT EDIT.
package neo

var pyMsgRegistry = map[uint16]string{
	1:	"RequestIdentification",
	3:	"Ping",
	5:	"CloseClient",
	6:	"AskPrimary",
	8:	"NotPrimaryMaster",
	9:	"NotifyNodeInformation",
	10:	"Recovery",
	12:	"LastIDs",
	14:	"AskPartitionTable",
	16:	"SendPartitionTable",
	17:	"NotifyPartitionChanges",
	18:	"StartOperation",
	19:	"StopOperation",
	20:	"UnfinishedTransactions",
	22:	"LockedTransactions",
	24:	"FinalTID",
	26:	"ValidateTransaction",
	27:	"BeginTransaction",
	29:	"FailedVote",
	30:	"FinishTransaction",
	32:	"LockInformation",
	34:	"InvalidateObjects",
	35:	"NotifyUnlockInformation",
	36:	"AskNewOIDs",
	38:	"NotifyDeadlock",
	39:	"RebaseTransaction",
	41:	"RebaseObject",
	43:	"StoreObject",
	45:	"AbortTransaction",
	46:	"StoreTransaction",
	48:	"VoteTransaction",
	50:	"AskObject",
	52:	"AskTIDs",
	54:	"AskTransactionInformation",
	56:	"AskObjectHistory",
	58:	"AskPartitionList",
	60:	"AskNodeList",
	62:	"SetNodeState",
	63:	"AddPendingNodes",
	64:	"TweakPartitionTable",
	65:	"SetClusterState",
	66:	"Repair",
	67:	"NotifyRepair",
	68:	"NotifyClusterInformation",
	69:	"AskClusterState",
	71:	"AskObjectUndoSerial",
	73:	"AskTIDsFrom",
	75:	"AskPack",
	77:	"CheckReplicas",
	78:	"CheckPartition",
	79:	"AskCheckTIDRange",
	81:	"AskCheckSerialRange",
	83:	"NotifyPartitionCorrupted",
	84:	"NotifyReady",
	85:	"AskLastTransaction",
	87:	"AskCheckCurrentSerial",
	89:	"NotifyTransactionFinished",
	90:	"Replicate",
	91:	"NotifyReplicationDone",
	92:	"AskFetchTransactions",
	94:	"AskFetchObjects",
	96:	"AddTransaction",
	97:	"AddObject",
	98:	"Truncate",
	32768:	"Error",
	32769:	"AcceptIdentification",
	32771:	"Pong",
	32774:	"AnswerPrimary",
	32778:	"AnswerRecovery",
	32780:	"AnswerLastIDs",
	32782:	"AnswerPartitionTable",
	32788:	"AnswerUnfinishedTransactions",
	32790:	"AnswerLockedTransactions",
	32792:	"AnswerFinalTID",
	32795:	"AnswerBeginTransaction",
	32798:	"AnswerTransactionFinished",
	32800:	"AnswerInformationLocked",
	32804:	"AnswerNewOIDs",
	32807:	"AnswerRebaseTransaction",
	32809:	"AnswerRebaseObject",
	32811:	"AnswerStoreObject",
	32814:	"AnswerStoreTransaction",
	32816:	"AnswerVoteTransaction",
	32818:	"AnswerObject",
	32820:	"AnswerTIDs",
	32822:	"AnswerTransactionInformation",
	32824:	"AnswerObjectHistory",
	32826:	"AnswerPartitionList",
	32828:	"AnswerNodeList",
	32837:	"AnswerClusterState",
	32839:	"AnswerObjectUndoSerial",
	32841:	"AnswerTIDsFrom",
	32843:	"AnswerPack",
	32847:	"AnswerCheckTIDRange",
	32849:	"AnswerCheckSerialRange",
	32853:	"AnswerLastTransaction",
	32855:	"AnswerCheckCurrentSerial",
	32860:	"AnswerFetchTransactions",
	32862:	"AnswerFetchObjects",
}
