// Simple in-memory cache storage
package engine

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
	"errors"
)

var (
	ErrInvalidKey     = errors.New("invalid key")
	ErrInvalidTTL     = errors.New("invalid ttl")
	ErrExpiredKey     = errors.New("key has expired")
	ErrTxClosed       = errors.New("tx closed")
	ErrDatabaseClosed = errors.New("database closed")
	ErrTxNotWritable  = errors.New("tx not writable")
)

type (
	engine struct {
		mu     sync.RWMutex
		// config *Config
		exps   *hash.Hash // hash of ttl keys
		// log    *Log

		closed  bool // state of the db
		// persist bool 

		strStore  *strStore
		hashStore *hashStore
		setStore  *setStore

		evictors []evictor // eraser
	}
)

func New(config *Config) (*engine, error) {

	config.validate()

	db := &engine{
		config:    config,
		strStore:  newStrStore(),
		setStore:  newSetStore(),
		hashStore: newHashStore(),
		exps:      hash.New(),
	}

	evictionInterval := config.evictionInterval()
	if evictionInterval > 0 {
		db.evictors = []evictor{
			newSweeperWithStore(db.strStore, evictionInterval),
			newSweeperWithStore(db.setStore, evictionInterval),
			newSweeperWithStore(db.hashStore, evictionInterval),
			newSweeperWithStore(db.zsetStore, evictionInterval),
		}
		for _, evictor := range db.evictors {
			go evictor.run(db.exps)
		}
	}

	// db.persist = config.Path != ""
	//if db.persist {
	//	opts := log.DefaultOptions
	//	opts.NoSync = config.NoSync

	//	l, err := aol.Open(config.Path, opts)
	//	if err != nil {
	//		return nil, err
	//	}

	//	db.log = l

		// loading from log
	//	err = db.load()
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	//return db, nil
//}

func (db *engine) setTTL(dType DataType, key string, ttl int64) {
	db.exps.HSet(dType, key, ttl)
}

func (db *engine) getTTL(dType DataType, key string) interface{} {
	return db.exps.HGet(dType, key)
}

func (db *engine) hasExpired(key string, dType DataType) (expired bool) {
	ttl := db.exps.HGet(dType, key)
	if ttl == nil {
		return
	}
	if time.Now().Unix() > ttl.(int64) {
		expired = true
	}
	return
}

func (db *engine) evict(key string, dType DataType) {
	ttl := db.exps.HGet(dType, key)
	if ttl == nil {
		return
	}

	var r *record
	if time.Now().Unix() > ttl.(int64) {
		switch dType {
		case String:
			r = newRecord([]byte(key), nil, StringRecord, StringRem)
			db.strStore.Delete([]byte(key))
		case Hash:
			r = newRecord([]byte(key), nil, HashRecord, HashHClear)
			db.hashStore.HClear(key)
		case Set:
			r = newRecord([]byte(key), nil, SetRecord, SetSClear)
			db.setStore.SClear(key)
		}

		if err := db.write(r); err != nil {
			panic(err)
		}

		db.exps.HDel(dType, key)
	}
}

func (db *engine) Close() error {
	db.closed = true
	for _, evictor := range db.evictors {
		evictor.stop()
	}
	if db.log != nil {
		err := db.log.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *engine) write(r *record) error {
	if db.log == nil {
		return nil
	}
	encVal, err := r.encode()
	if err != nil {
		return err
	}

	return db.log.Write(encVal)
}