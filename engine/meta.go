package engine

type DataType = string

const (
	String DataType = "String"
	Hash   DataType = "Hash"
	Set    DataType = "Set"
)

const (
	StringRecord uint16 = iota
	HashRecord
	SetRecord
)

// Available actions
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
)

const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)
