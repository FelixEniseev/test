// try to use meta info instead of separate fields/structs/etc

package engine

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrInvalidEntry = errors.New("invalid entry")
)

const (
	// keySize 4b + valueSize 4b + timestamp 8b + state 2b
	entryHeaderSize = 18
)

type (
	record struct {
		meta      *meta
		state     uint16 // data type + mark
		timestamp uint64 
	}

	// Meta itself.
	meta struct {
		key        []byte
		value      []byte
		keySize    uint32
		valueSize  uint32
	}
)

func newInternal(key, value []byte, state uint16, timestamp uint64) *record {
	return &record{
		state: state, timestamp: timestamp,
		meta: &meta{
			key:        key,
			value:      value,
			keySize:    uint32(len(key)),
			valueSize:  uint32(len(value)),
		},
	}
}

func newRecord(key, []byte, t, mark uint16) *record {
	var state uint16 = 0
	// data type + mark
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, nil, state, uint64(time.Now().UnixNano()))
}

func newRecordWithValue(key, value []byte, t, mark uint16) *record {
	var state uint16 = 0
	// again
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, value, state, uint64(time.Now().UnixNano()))
}

func newRecordWithExpire(key []byte, deadline int64, t, mark uint16) *record {
	var state uint16 = 0
	// and again
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, nil, state, uint64(deadline))
}

func (e *record) size() uint32 {
	return entryHeaderSize + e.meta.keySize + e.meta.valueSize
}

// 
func (e *record) encode() ([]byte, error) {
	if e == nil || e.meta.keySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks := e.meta.keySize
	vs := e.meta.valueSize
	buf := make([]byte, e.size())

	binary.BigEndian.PutUint32(buf[0:4], ks)
	binary.BigEndian.PutUint32(buf[4:8], vs)
	binary.BigEndian.PutUint16(buf[8:10], e.state)
	binary.BigEndian.PutUint64(buf[10:18], e.timestamp)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.meta.key)
	if vs > 0 {
		copy(buf[(entryHeaderSize+ks):(entryHeaderSize+ks+vs)], e.meta.value)
	}

	return buf, nil
}

func decode(buf []byte) (*record, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	state := binary.BigEndian.Uint16(buf[8:10])
	timestamp := binary.BigEndian.Uint64(buf[10:18])

	return &record{
		meta: &meta{
			keySize:    ks,
			valueSize:  vs,
			key:        buf[entryHeaderSize : entryHeaderSize + ks],
			value:      buf[(entryHeaderSize + ks):(entryHeaderSize + ks + vs)],
		},
		state:     state,
		timestamp: timestamp,
	}, nil
}

func (e *record) getType() uint16 {
	return e.state >> 8
}

func (e *record) getMark() uint16 {
	return e.state & (2<<7 - 1)
}