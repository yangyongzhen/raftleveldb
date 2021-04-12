package raft

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	SingleClusterFlag = true
	proposeC          = make(chan string)
	Leaderid uint64
	CurrentNodeid uint64
	IsLeader bool
	confChangeC      = make(chan raftpb.ConfChange)
	commitTempSetMap sync.Map
	commitTempDelMap sync.Map
)

const (
	//删除标记
	DelFlag = "DELFlag"
	timeout = 10000
)

type Item struct {
	//value值
	Object string
	//租约时间
	Expiration int64
	//数据是否同步到其他集群节点，false 未同步，true：已同步
	SyncFlag bool
}

//defer close(confChangeC)
type KVAPI struct {
	Store       *kvstore
	ConfChangeC chan<- raftpb.ConfChange
}

type Response struct {
	key string
	err error
}

//初始化raft集群
func (h *KVAPI) InitRaftCluster(id int, cluster string, join bool) *kvstore {
	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	peers := strings.Split(cluster, ",")
	if len(peers) == 1 {
		//节点
		SingleClusterFlag = true
	} else {
		//多节点raft集群
		SingleClusterFlag = false
	}
	CurrentNodeid=uint64(id)
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(id, peers, join, getSnapshot, proposeC, confChangeC)
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)
	return kvs
}

//异步增加
func (h *KVAPI) Put(key, value string, d time.Duration) error {
	if err := h.Store.Propose(key, value, d); err != nil {
		return err
	}
	return nil
}

//同步增加
func (h *KVAPI) SyncPut(key, value string, d time.Duration) error {
	var err error
	var count int
	//T1:=time.Now()
	h.Store.firstLoadFlag = false
	err = h.Store.Propose(key, value, d)
	if SingleClusterFlag {
		return err
	} else {
		for {
			count++
			//获取raft一致性提交结果
			temperr, ok := commitTempSetMap.Load(key)
			if ok {
				if temperr == nil {
					err = nil
				} else {
					err = temperr.(error)
				}
				commitTempSetMap.Delete(key)
				//T2:=time.Now()
				//log.Println("------------------------------------整体syncput耗时(纳秒):",strconv.Itoa(int(T2.Sub(T1).Nanoseconds())),",key:",key)
				return err
			}
			//每隔300微秒检查raft一致性提交结果
			time.Sleep(300 * time.Microsecond)
			if count >= timeout {
				//超时
				return fmt.Errorf("SyncPut超时key:%s,commitTempSetMap:%v", key, commitTempSetMap)
			}
		}
	}

}
func (h *KVAPI) Delete(key string) error {
	var err error
	var count int
	if SingleClusterFlag {
		if err := h.Store.Delete(key); err != nil {
			return fmt.Errorf("删除数据失败：" + err.Error())
		} else {
			return nil
		}
	} else {
		h.Store.Delete(key)
		for {
			temperr, ok := commitTempDelMap.Load(key)
			if ok {
				if temperr == nil {
					err = nil
				} else {
					err = temperr.(error)
				}
				commitTempDelMap.Delete(key)
				return err
			}
			time.Sleep(500 * time.Microsecond)
			if count >= timeout {
				//超时时间1秒
				return fmt.Errorf("del超时,key:%s,commitTempDelMap:%v", key, commitTempDelMap)
			}
		}
	}
}

func (h *KVAPI) DeleteExpired() {
	h.Store.DeleteExpired()
}

func (h *KVAPI) Get(key string) (string, error) {
	if v, ok := h.Store.Lookup(key); !ok {
		return "", fmt.Errorf("获取key失败!")
	} else {
		return v, nil
	}
}

func (h *KVAPI) Keys() ([]string, error) {
	if v, ok := h.Store.Keys(); !ok {
		return nil, fmt.Errorf("获取所有key失败!")
	} else {
		return v, nil
	}
}

func (h *KVAPI) GetLeaderFlag() (bool) {
	return IsLeader
}
