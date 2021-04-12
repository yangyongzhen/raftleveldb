package raft
/*
import (
	context "context"
	"fmt"
	"log"
	"net"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	//"services/tradefrontservice/cache/leveldbraft/raft/grpcplugin"
)

type grpcKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (h *grpcKVAPI) Get(ctx context.Context, in *grpcplugin.GetRequest) (*grpcplugin.GetReply, error) {
	var recode int32=-1
	var err error
	v, ok := h.store.Lookup(string(in.Key))
	if ok {
		//println("grpcGetkey:",string(in.Key),"value:",v)
		recode = 100
		err=nil
	}else{
		println("grpcgetkey:",string(in.Key),"失败")
		err=fmt.Errorf("grpcgetkey:"+string(in.Key)+"失败")
		recode = -1
	}
	return &grpcplugin.GetReply{Value: []byte(v), Error: recode}, err
}

func (h *grpcKVAPI) Set(ctx context.Context, in *grpcplugin.SetRequest) (*grpcplugin.ErrorReply, error) {
	var recode int32=100
	//println("grpcSetkey:",string(in.Key),"value:",string(in.Value))
	h.store.Propose(string(in.Key), string(in.Value))
	return &grpcplugin.ErrorReply{Error: recode}, nil
}

func (s *grpcKVAPI) Has(ctx context.Context, in *grpcplugin.GetRequest) (*grpcplugin.HasReply, error) {

	return &grpcplugin.HasReply{Value: false, Error: -1}, nil
}

func (s *grpcKVAPI) Del(ctx context.Context, in *grpcplugin.GetRequest) (*grpcplugin.ErrorReply, error) {

	return &grpcplugin.ErrorReply{Error: -1}, nil
}
*/

/*
type LevelRpcServer struct {
	Port uint
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func RpcServer(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) *LevelRpcServer {
	return &LevelRpcServer{
		Port: uint(port),
		store:       kv,
		confChangeC: confChangeC,
	}
}

func (self *LevelRpcServer) Listen() {
	var err error
	port := strconv.FormatUint(uint64(self.Port), 10)
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	grpcplugin.RegisterLevelRpcServiceServer(server, &grpcKVAPI{self.store,self.confChangeC})
	reflection.Register(server)
	log.Printf("RPC Listen at :%s", port)
	err = server.Serve(listen)
	if err != nil {
		panic(err)
	}
}
*/