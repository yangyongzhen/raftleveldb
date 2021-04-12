package raftleveldb

import (
	"fmt"
	"log"
	//"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"raftleveldb/raft"
)

//全局缓存
var (
	leveldabRaftHttpclient HTTPClient
	leveldabRaftProcess    *raft.KVAPI
	err                    error
	synclock               sync.Mutex
	mu                     sync.RWMutex
)

type LeveldbRaftStru struct {
	//集群列表
	Endpoints []string `json:"endpoints"`
	//当前集群ID
	ID int `json:"id"`
	//是否加入集群
	Join bool `json:"join"`
	//连接超时时间
	DialTimeout time.Duration `json:"dial-timeout"`
	//上下文超时时间
	OpTimeout time.Duration `json:"op_timeout"`
}

func NewLeveldbRaft(dialTimeout time.Duration, opTimeout time.Duration, endpoints []string, id int, join bool) *LeveldbRaftStru {
	return &LeveldbRaftStru{
		Endpoints:   endpoints,
		ID:          id,
		Join:        join,
		DialTimeout: dialTimeout,
		OpTimeout:   opTimeout,
	}
}

//初始化raft集群
func (c *LeveldbRaftStru) NewConn() error {
	leveldabRaftProcess = &raft.KVAPI{
		ConfChangeC: make(chan<- raftpb.ConfChange),
	}
	kvstore := leveldabRaftProcess.InitRaftCluster(c.ID, c.Endpoints[0], c.Join)
	leveldabRaftProcess.Store = kvstore
	// wait server started
	<-time.After(time.Second * 3)
	return nil
}

//获取缓存值
func (c *LeveldbRaftStru) Get(key interface{}) (interface{}, bool) {
	//获取该key为前缀的所有key-value
	valueStr, err := leveldabRaftProcess.Get(key.(string))
	if err != nil {
		return nil, false
	}
	return valueStr, true
}

//同步增加
func (c *LeveldbRaftStru) Add(key interface{}, value interface{}) bool {
	//序列化成字符串
	//T1:=time.Now()
	bvalue, err := jsoniter.Marshal(value)
	if err != nil {
		log.Printf("序列化失败:", err.Error(), value)
		return false
	}
	err = leveldabRaftProcess.SyncPut(key.(string), string(bvalue), 0)
	//T2:=time.Now()
	//log.Println("------------------------------------整体SET-ADD耗时(纳秒):",strconv.Itoa(int(T2.Sub(T1).Nanoseconds())),",key:",key.(string))
	if err != nil {
		println("add失败:", err.Error())
		return false
	} else {
		return true
	}
}

//异步增加
func (c *LeveldbRaftStru) AsynAdd(key interface{}, value interface{}) bool {
	//序列化成字符串
	bvalue, err := jsoniter.Marshal(value)
	if err != nil {
		log.Printf("序列化失败:", err.Error(), value)
		return false
	}
	err = leveldabRaftProcess.Put(key.(string), string(bvalue), 0)
	if err != nil {
		println("add失败:", err.Error())
		return false
	} else {
		return true
	}
}

//同步增加缓存带租约过期时间
func (c *LeveldbRaftStru) AddWithExpiration(key interface{}, value interface{}, d time.Duration) bool {
	//序列化成字符串
	bvalue, err := jsoniter.Marshal(value)
	if err != nil {
		log.Printf("add序列化失败:", err.Error(), value)
		return false
	}
	err = leveldabRaftProcess.SyncPut(key.(string), string(bvalue), d)
	if err != nil {
		println("add失败:", err.Error())
		return false
	} else {
		return true
	}
}

//同步更新缓存
func (c *LeveldbRaftStru) Update(key interface{}, value interface{}) bool {
	bvalue, err := jsoniter.Marshal(value)
	if err != nil {
		log.Printf("update序列化失败:", err.Error())
		return false
	}
	err = leveldabRaftProcess.SyncPut(key.(string), string(bvalue), 0)
	if err != nil {
		return false
	} else {
		return true
	}
}

//异步更新缓存
func (c *LeveldbRaftStru) AsynUpdate(key interface{}, value interface{}) bool {
	bvalue, err := jsoniter.Marshal(value)
	if err != nil {
		log.Printf("update序列化失败:", err.Error())
		return false
	}
	err = leveldabRaftProcess.Put(key.(string), string(bvalue), 0)
	if err != nil {
		return false
	} else {
		return true
	}
}

//同步更新缓存 带租约过期时间
func (c *LeveldbRaftStru) UpdateWithExpiration(key interface{}, value interface{}, d time.Duration) bool {
	bvalue, err := jsoniter.Marshal(value)
	if err != nil {
		log.Printf("update序列化失败:", err.Error())
		return false
	}
	err = leveldabRaftProcess.SyncPut(key.(string), string(bvalue), d)
	if err != nil {
		return false
	} else {
		return true
	}
}

//删除key
func (c *LeveldbRaftStru) Del(key interface{}) bool {
	err := leveldabRaftProcess.Delete(key.(string))
	if err != nil {
		return false
	}
	return true
}

//缓存失效也false
func (c *LeveldbRaftStru) Exits(key interface{}) bool {
	_, err := leveldabRaftProcess.Get(key.(string))
	if err != nil {
		return false
	} else {
		return true
	}
}

func (c *LeveldbRaftStru) LoadCache() error {
	return err
}
func (c *LeveldbRaftStru) SaveCache() error {
	return err
}

//返回缓存数量
func (c *LeveldbRaftStru) Len() int {
	return 0
}

//返回缓存所有keys不包括已过失效期效key
func (c *LeveldbRaftStru) Keys() []interface{} {
	var keys = make([]interface{}, 0)
	keyBuf, err := leveldabRaftProcess.Keys()
	if err != nil {
		return nil
	}
	for _, v := range keyBuf {
		keys = append(keys, v)
	}
	return keys
}
//删除所有租约过期数据
func (c *LeveldbRaftStru) DeleteExpired() {
	leveldabRaftProcess.DeleteExpired()
}

//根据结构体字段名以及对应值返回结构实例
func (c *LeveldbRaftStru) GetByCondition(tableNmes string, field string, fieldvalue string) (interface{}, bool) {
	cashKeys := c.Keys()
	for _, cashkey := range cashKeys {
		if strings.Contains(cashkey.(string), tableNmes) {
			value, _ := c.Get(cashkey)
			//获取结构体实例的反射类型对象
			typeOfStru := reflect.TypeOf(value)
			if typeOfStru.Kind() == reflect.Ptr {
				typeOfStru = typeOfStru.Elem()
			}
			if typeOfStru.Kind() != reflect.Struct {
				return nil, false
			}
			// 通过字段名, 找到字段类型信息
			fieldNum := typeOfStru.NumField()
			for i := 0; i < fieldNum; i++ {
				//比较字段名称
				if strings.ToUpper(typeOfStru.Field(i).Name) == strings.ToUpper(field) {
					//反射值
					v := reflect.ValueOf(value)
					val := v.FieldByName(typeOfStru.Field(i).Name).String()
					//比较传入的值与结构体实例中值是否相等，如果相等，返回结构体实例
					if val == fieldvalue {
						return value, true
					} else {
						continue
					}
				} else {
					continue
				}
			}

		} else {
			continue
		}
	}
	return nil, false
}

//根据结构体字段名以及对应值返回结构实例
func (c *LeveldbRaftStru) GetByConditionMap(tableNmes string, fieldmap map[string]string) (interface{}, bool) {
	synclock.Lock()
	defer func() (interface{}, bool) { //必须捕获panic异常 否则消息处理错误会造成程序崩溃
		var errmsg string = "根据结构体字段名以及对应值返回结构实例异常"
		if e := recover(); e != nil {
			errmsg += fmt.Sprintf(":%v", e)
			//util.Log.Errorf(errmsg)
			fmt.Printf("%v", e)
		}
		synclock.Unlock()
		return nil, false
	}()
	//获取缓存中所有keys
	cashKeys := c.Keys()
	for _, cashkey := range cashKeys {
		//缓存key中是否包含表名
		if strings.Contains(cashkey.(string), tableNmes) {
			value, _ := c.Get(cashkey)
			v := reflect.ValueOf(value)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			//获取结构体实例的反射类型对象
			typeOfStru := reflect.TypeOf(value)
			if typeOfStru.Kind() == reflect.Ptr {
				typeOfStru = typeOfStru.Elem()
			}
			if typeOfStru.Kind() != reflect.Struct {
				return nil, false
			}
			var isok bool = false
			//遍历所有key字段和值是否匹配
			for field, fieldvalue := range fieldmap {
				isok = false
				// 通过字段名, 找到字段类型信息
				fieldNum := typeOfStru.NumField()
				for i := 0; i < fieldNum; i++ {
					//比较字段名称
					if strings.ToUpper(typeOfStru.Field(i).Name) == strings.ToUpper(field) {
						//判断字段反射类型
						datatype := v.FieldByName(field).Kind()
						if datatype == reflect.Int64 || datatype == reflect.Int {
							val := v.FieldByName(field).Int()
							//比较传入的值与结构体实例中值是否相等，如果相等，返回结构体实例
							if strconv.Itoa(int(val)) == fieldvalue {
								isok = true
								break
							} else {
								//字段值不等继续
								continue
							}
						} else {
							val := v.FieldByName(field).String()
							//比较传入的值与结构体实例中值是否相等，如果相等，返回结构体实例
							if val == fieldvalue {
								isok = true
								break
							} else {
								//字段值不等继续
								continue
							}
						}
					} else {
						//字段名字不符继续
						continue
					}
				}
				if !isok {
					//有一个字段条件不满足返回验证失败
					return nil, false
				}
			}
			if isok {
				//所有字段条件满足返回成功
				return value, true
			}
		} else {
			//没有包含表名的Key继续
			continue
		}
	}
	//遍历所有没有匹配返回失败
	return nil, false
}

//获取当前节点是否为raft集群leader,true：leader false：follow
func (c *LeveldbRaftStru) IsLeaderFlag() bool {
	return leveldabRaftProcess.GetLeaderFlag()
}
