// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	//"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	levelAdmin "github.com/qjues/leveldb-admin"
	"github.com/syndtr/goleveldb/leveldb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC      chan<- string // channel for proposing updates
	mu            sync.RWMutex
	kvStore       map[string]string // current committed key-value pairs
	leveldb       *leveldb.DB
	snapshotter   *snap.Snapshotter
	firstLoadFlag bool
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	db, err := leveldb.OpenFile("./leveldb", nil)
	//db, err := leveldb.OpenFile("leveldb", nil)
	if err != nil {
		println(err.Error())
		panic(err)
	}
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), leveldb: db, snapshotter: snapshotter, firstLoadFlag: true}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	//定时进行集群数据同步
	//go s.CheckdbSync()
	//leveldb UI界面管理工具
	levelAdmin.GetLevelAdmin().Register(db, "description").Start()
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var v string
	vb, err := s.leveldb.Get([]byte(key), nil)
	if err != nil {
		log.Println("leveldb获取失败,key:", key, ",信息：", err.Error())
		return "", false
	} else {
		valueItem := &Item{}
		err := jsoniter.Unmarshal(vb, valueItem)
		if err != nil {
			log.Println("反序列化失败:", err.Error())
			return "", false
		} else {
			//是否有租约
			if valueItem.Expiration > 0 {
				//当前时间大于租约时间，已过期
				if time.Now().UnixNano() > valueItem.Expiration {
					//租约过期不返回
					return "", false
				}
			}
			v = valueItem.Object
			return v, true
		}
	}
}

func (s *kvstore) Keys() ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keySilce := make([]string, 0)
	iter := s.leveldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := string(iter.Key())
		valueItem := &Item{}
		err := jsoniter.Unmarshal(iter.Value(), valueItem)
		if err != nil {
			log.Println("反序列化失败:", err.Error())
			continue
		} else {
			if valueItem.Expiration > 0 {
				//当前时间大于租约时间，已过期
				if time.Now().UnixNano() > valueItem.Expiration {
					//租约过期不返回
					continue
				}
			}
			//key = valueItem.Object
			keySilce = append(keySilce, key)
		}
	}

	return keySilce, true
}

func (s *kvstore) Propose(k string, v string, d time.Duration) error {
	var e int64
	if d > 0 {
		//增加租约时间
		e = time.Now().Add(d).UnixNano()
	}
	valueItem := Item{
		Object:     v,
		Expiration: e,
	}
	//结构序列化成字符串
	valuebuf, err := jsoniter.Marshal(valueItem)
	if err != nil {
		log.Println("反序列化失败:", err.Error())
		return err
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, string(valuebuf)}); err != nil {
		log.Println("二进制流序列化失败:", err.Error())
		return err
	}
	s.proposeC <- buf.String()
	if SingleClusterFlag {
		s.mu.Lock()
		defer s.mu.Unlock()
		//数据是否同步到其他集群标记
		valueItem.SyncFlag = false
		valuebufdb, err := jsoniter.Marshal(valueItem)
		if err != nil {
			log.Println("序列化失败:", err.Error())
			return err
		}
		err = s.leveldb.Put([]byte(k), valuebufdb, nil)
		if err != nil {
			log.Println("保存leveldb数据失败:", err.Error(), "key:", k, "value:", v)
			return err
		}
	}
	return nil
}

func (s *kvstore) Delete(k string) error {
	if SingleClusterFlag {
		s.mu.Lock()
		defer s.mu.Unlock()
		err := s.leveldb.Delete([]byte(k), nil)
		if err != nil {
			log.Println("删除leveldb数据失败:", err.Error(), "key:", k)
			return err
		} else {
			return nil
		}
	}
	//多集群同步到其他节点做删除操作 key前加 DEL标识符
	keystr := DelFlag + "/" + k
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{keystr, ""}); err != nil {
		log.Println("二进制流序列化失败:", err.Error())
		return err
	}
	s.proposeC <- buf.String()
	return nil
}

//删除所有租约过期数据
func (s *kvstore) DeleteExpired() {
	s.mu.Lock()
	s.mu.Unlock()
	iter := s.leveldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := string(iter.Key())
		valueItem := &Item{}
		err := jsoniter.Unmarshal(iter.Value(), valueItem)
		if err != nil {
			log.Println("反序列化失败:", err.Error())
			continue
		} else {
			if valueItem.Expiration > 0 {
				now := time.Now().UnixNano()
				//当前时间大于租约时间，已过期
				if now > valueItem.Expiration {
					//租约过期删除
					s.Delete(key)
				}
			}
		}
	}
}

//循环监听通道，并从其中取出日志的函数。并且如果本地存在snapshot，则先将日志重放到内存状态机中。
func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		// data如果为nil就表示要加载snapshot
		if commit == nil {
			// signaled to load snapshot
			// 加载snapshot
			snapshot, err := s.loadSnapshot()
			//如果是其他错误，就抛出异常
			if err != nil {
				log.Println(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				//将之前某时刻快照重新设置为状态机目前的状态
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Println(err)
				}
			}
			continue
		}
		for _, data := range commit.data {
			//T9:=time.Now()
			//先对数据解码
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Printf("raft: could not decode message:%v", err)
			}
			s.mu.Lock()
			s.mu.Unlock()
			valueItem := &Item{}
			err := jsoniter.Unmarshal([]byte(dataKv.Val), valueItem)
			valueItem.SyncFlag = true
			valuebuf, err := jsoniter.Marshal(valueItem)
			if err != nil {
				log.Println("数据序列化失败:", err.Error()+"key:", dataKv.Key)
			} else {
				if strings.HasPrefix(dataKv.Key, DelFlag) {
					keybuf := strings.Split(dataKv.Key, "/")
					err = s.leveldb.Delete([]byte(keybuf[1]), nil)
					if err != nil {
						log.Println("删除leveldb数据失败:", err.Error(), "key:", dataKv.Key, "value:", string(valuebuf))
					}
				} else {
					err = s.leveldb.Put([]byte(dataKv.Key), valuebuf, nil)
					if err != nil {
						log.Println("保存leveldb数据失败:", err.Error(), "key:", dataKv.Key, "value:", string(valuebuf))
					}
				}
			}
			//首次从wal加载预写日志不执行
			if !s.firstLoadFlag {
				//删除操作
				if strings.HasPrefix(dataKv.Key, DelFlag) {
					keybuf := strings.Split(dataKv.Key, "/")
					commitTempDelMap.Store(keybuf[1], err)
				} else {
					//增加操作
					commitTempSetMap.Store(dataKv.Key, err)
					//log.Println(dataKv.Key,err)
				}
			}
		}
		close(commit.applyDoneC)
	}
	// 如果error channel有错误
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	// 产生snapshaot的时候必须加上读写锁
	s.mu.RLock()
	defer s.mu.RUnlock()
	iter := s.leveldb.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())
		s.kvStore[key] = value
	}
	iter.Release()
	//将kv序列化为Json
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

//从snapshot中恢复kv存储(恢复应用状态机)
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	// 对snapshot反序列化
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	//s.kvStore = store
	for k, v := range store {
		err := s.leveldb.Put([]byte(k), []byte(v), nil)
		if err != nil {
			log.Println("恢复快照数据失败:", err.Error())
		}
	}
	return nil
}

func (s *kvstore) CheckdbSync() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.RLock()
			defer s.mu.RUnlock()
			iter := s.leveldb.NewIterator(nil, nil)
			for iter.Next() {
				key := string(iter.Key())
				valueItem := &Item{}
				err := jsoniter.Unmarshal(iter.Value(), valueItem)
				if err != nil {
					log.Println("反序列化失败:", err.Error())
					continue
				}
				reSendValueItem := Item{
					Object:     valueItem.Object,
					Expiration: valueItem.Expiration,
				}
				if !valueItem.SyncFlag {
					//结构序列化成字符串
					//util.Log.Info("开始重新同步集群数据:","key:",key,",value:",reSendValueItem.Object,",Expiration:",valueItem.Expiration)
					valuebuf, err := jsoniter.Marshal(reSendValueItem)
					if err != nil {
						log.Println("序列化失败:", err.Error())
						continue
					}
					var buf bytes.Buffer
					if err := gob.NewEncoder(&buf).Encode(kv{key, string(valuebuf)}); err != nil {
						log.Println("二进制流序列化失败:", err.Error())
						continue
					}
					s.proposeC <- buf.String()
					//util.Log.Info("同步集群数据完成:",buf.String())
					//println("同步集群数据完成:",reSendValueItem)
				}
			}
			iter.Release()
		}
	}
}
