package raftleveldb
/*
import (
	"context"
	"time"

	"services/tradefrontservice/cache/leveldbraft/raft/grpcplugin"
)
var GClient grpcplugin.LevelRpcServiceClient
type GRPCClient struct{
	DialTimeout time.Duration `json:"dial-timeout"`
	//上下文超时时间
	OpTimeout   time.Duration `json:"op_timeout"`
}
func (grpcClient *GRPCClient)Put(key, value string) (string, error) {
	in:=&grpcplugin.SetRequest{
		Key:[]byte(key),
		Value: []byte(value),
	}
	_,err=GClient.Set(context.Background(),in)
	if err != nil {
		return "",err
	}
	return "",nil
}

func (grpcClient *GRPCClient)Get(key string) (string, error) {
	in:=&grpcplugin.GetRequest{
		Key:[]byte(key),
	}
	out,err:=GClient.Get(context.Background(),in)
	if err != nil {
		return "",err
	}
	return string(out.Value) ,nil
}

*/