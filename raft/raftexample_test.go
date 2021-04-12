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
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type cluster struct {
	peers       []string
	commitC     []<-chan *string
	errorC      []<-chan error
	proposeC    []chan string
	confChangeC []chan raftpb.ConfChange
}

// newCluster creates a cluster of n nodes
func newCluster(n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}

	clus := &cluster{
		peers:       peers,
		commitC:     make([]<-chan *string, len(peers)),
		errorC:      make([]<-chan error, len(peers)),
		proposeC:    make([]chan string, len(peers)),
		confChangeC: make([]chan raftpb.ConfChange, len(peers)),
	}

	for i := range clus.peers {
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)
		//clus.commitC[i], clus.errorC[i], _ = newRaftNode(i+1, clus.peers, false, nil, clus.proposeC[i], clus.confChangeC[i])
	}

	return clus
}

// sinkReplay reads all commits in each node's local log.
func (clus *cluster) sinkReplay() {
	for i := range clus.peers {
		for s := range clus.commitC[i] {
			if s == nil {
				break
			}
		}
	}
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		close(clus.proposeC[i])
		for range clus.commitC[i] {
			// drain pending commits
		}
		// wait for channel to close
		if erri := <-clus.errorC[i]; erri != nil {
			err = erri
		}
		// clean intermediates
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
	}
	return err
}

func (clus *cluster) closeNoErrors(t *testing.T) {
	if err := clus.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestProposeOnCommit starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
/*func TestProposeOnCommit(t *testing.T) {
	clus := newCluster(3)
	defer clus.closeNoErrors(t)

	clus.sinkReplay()

	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(pC chan<- string, cC <-chan *string, eC <-chan error) {
			for n := 0; n < 100; n++ {
				s, ok := <-cC
				if !ok {
					pC = nil
				}
				select {
				case pC <- *s:
					continue
				case err := <-eC:
					t.Errorf("eC message (%v)", err)
				}
			}
			donec <- struct{}{}
			for range cC {
				// acknowledge the commits from other nodes so
				// raft continues to make progress
			}
		}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

		// one message feedback per node
		go func(i int) { clus.proposeC[i] <- "foo" }(i)
	}

	for range clus.peers {
		<-donec
	}
}

// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(1)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

// TestCloseProposerInflight tests closing the producer while
// committed messages are being published to the client.
func TestCloseProposerInflight(t *testing.T) {
	clus := newCluster(1)
	defer clus.closeNoErrors(t)

	clus.sinkReplay()

	// some inflight ops
	go func() {
		clus.proposeC[0] <- "foo"
		clus.proposeC[0] <- "bar"
	}()

	// wait for one message
	if c, ok := <-clus.commitC[0]; *c != "foo" || !ok {
		t.Fatalf("Commit failed")
	}
}
*/

/*func TestPutAndGetKeyValue(t *testing.T) {
	clusters := []string{"http://127.0.0.1:9021"}

	proposeC := make(chan string)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(1, clusters, false, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	srv := httptest.NewServer(&httpKVAPI{
		store:       kvs,
		confChangeC: confChangeC,
	})
	defer srv.Close()

	// wait server started
	<-time.After(time.Second * 3)
	wantKey, wantValue := "test-key4", "test-value4"
	url := fmt.Sprintf("%s/%s", srv.URL, wantKey)
	body := bytes.NewBufferString(wantValue)
	cli := srv.Client()

	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/html; charset=utf-8")
	_, err = cli.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(time.Second)

	resp, err := cli.Get(url)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if gotValue := string(data); wantValue != gotValue {
		t.Fatalf("expect %s, got %s", wantValue, gotValue)
	}
}*/


/*func TestGRPCPutAndGetKeyValue(t *testing.T) {

	serverAddr := "127.0.0.1:9121"
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := NewLevelRpcServiceClient(conn)
setin:=&SetRequest{
	Key:[]byte("test001"),
	Value: []byte("kjud"),
}
	_,err=client.Set(context.Background(),setin)
	if err!=nil {
		println(err)
	}
	getin:=&GetRequest{
		Key:[]byte("test001"),
	}
	out,err:=client.Get(context.Background(),getin)

	if err!=nil {
		println(err)
	}
	println(string(out.Value))

}*/

/*func BenchmarkPutKeyValue(b *testing.B) {

	cli:=http.DefaultClient
	for i := 0; i < b.N; i++ {
		wantKey, wantValue := "test-key"+strconv.Itoa(i), "test-value"+strconv.Itoa(i)
		url := fmt.Sprintf("%s/%s", "http://192.168.61.204:12379", wantKey)
		body := bytes.NewBufferString(wantValue)
		req, err := http.NewRequest("PUT", url, body)
		if err != nil {
			println(err)
		}
		req.Header.Set("Content-Type", "text/html; charset=utf-8")
		_, err = cli.Do(req)
		if err != nil {
			println(err)
		}
	}
}
func BenchmarkGetKeyValue(b *testing.B) {
	b.StopTimer()
	cli:=http.DefaultClient
	wantKey, wantValue := "test-key0", "test-value0"
	url := fmt.Sprintf("%s/%s", "http://192.168.61.204:12379", wantKey)
	for i := 0; i < b.N; i++ {
		resp, err := cli.Get(url)
		if err != nil {
			println(err)
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			println(err)
		}
		defer resp.Body.Close()
		if gotValue := string(data); wantValue != gotValue {
			println("expect %s, got %s", wantValue, gotValue)
		}
	}


}
*/

/*func BenchmarkGrpcPutKeyValue(b *testing.B) {
	b.StopTimer()
	serverAddr := "192.168.61.204:12379"
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := NewLevelRpcServiceClient(conn)
	setin:=&SetRequest{
		Key:[]byte("test001|001|87236342"),
		Value: []byte("{\\\"ID\\\":0,\\\"USERID\\\":43097,\\\"ACCNO\\\":\\\"70636\\\",\\\"ACCTYPE\\\":0,\\\"CARDNO\\\":\\\"5330\\\",\\\"CARDID\\\":\\\"\\\",\\\"CARDBAL\\\":24968,\\\"BALANCE\\\":24968,\\\"QRYPASSWD\\\":\\\"1296DC193E9DBE33\\\",\\\"TXPASSWD\\\":\\\"1296DC193E9DBE33\\\",\\\"PASSCHKTOL\\\":0,\\\"TOTALDEPOSITE\\\":0,\\\"PLEDGECODE\\\":\\\"\\\",\\\"USECARDTIMES\\\":0,\\\"CDBALCHK\\\":\\\"\\\",\\\"BALCHK\\\":\\\"\\\",\\\"CARDSUBNUM\\\":0,\\\"DBSUBNUM\\\":0,\\\"ACCSTATUS\\\":\\\"1\\\",\\\"CMPACCSTS\\\":\\\"\\\",\\\"CMPACCTIMES\\\":0,\\\"INPUTUSERID\\\":0,\\\"PCODE\\\":\\\"\\\",\\\"OPENAMT\\\":0,\\\"CARDNUM\\\":0,\\\"ORGANCODE\\\":\\\"15\\\",\\\"CARDSN\\\":0}\"}"),
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_,err=client.Set(context.Background(),setin)
		if err!=nil {
			println(err)
		}
	}
	conn.Close()
}
func BenchmarkGrpcGetKeyValue(b *testing.B) {
	b.StopTimer()
	serverAddr := "192.168.61.204:12379"
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := NewLevelRpcServiceClient(conn)
	getin:=&GetRequest{
		Key:[]byte("test001"),
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_,err:=client.Get(context.Background(),getin)
		if err!=nil {
			println(err)
		}
	}
	conn.Close()
}
*/
/*func BenchmarkGetKeyValue(b *testing.B) {
	b.StopTimer()
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	kvapi:=&KVAPI{}
	kvstore:=kvapi.RaftInit(*id,*cluster,*join)
	var conf=make(chan<- raftpb.ConfChange)
	// wait server started
	<-time.After(time.Second * 3)
	kvapi.store=kvstore
	kvapi.confChangeC=conf

	kvapi.PUT("test001","334324yyyy")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_,err:=kvapi.Get("test001")
		if err!=nil {
			println(err)
		}
	}
}*/
func BenchmarkPutKeyValue(b *testing.B) {
	b.StopTimer()
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	kvapi:=&KVAPI{}
	kvstore:=kvapi.InitRaftCluster(*id,*cluster,*join)
	var conf=make(chan<- raftpb.ConfChange)
	<-time.After(time.Second * 3)
	kvapi.Store=kvstore
	kvapi.ConfChangeC=conf
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err:=kvapi.SyncPut("test00"+strconv.Itoa(i),"{\\\"ID\\\":0,\\\"USERID\\\":43097,\\\"ACCNO\\\":\\\"70636\\\",\\\"ACCTYPE\\\":0,\\\"CARDNO\\\":\\\"5330\\\",\\\"CARDID\\\":\\\"\\\",\\\"CARDBAL\\\":24968,\\\"BALANCE\\\":24968,\\\"QRYPASSWD\\\":\\\"1296DC193E9DBE33\\\",\\\"TXPASSWD\\\":\\\"1296DC193E9DBE33\\\",\\\"PASSCHKTOL\\\":0,\\\"TOTALDEPOSITE\\\":0,\\\"PLEDGECODE\\\":\\\"\\\",\\\"USECARDTIMES\\\":0,\\\"CDBALCHK\\\":\\\"\\\",\\\"BALCHK\\\":\\\"\\\",\\\"CARDSUBNUM\\\":0,\\\"DBSUBNUM\\\":0,\\\"ACCSTATUS\\\":\\\"1\\\",\\\"CMPACCSTS\\\":\\\"\\\",\\\"CMPACCTIMES\\\":0,\\\"INPUTUSERID\\\":0,\\\"PCODE\\\":\\\"\\\",\\\"OPENAMT\\\":0,\\\"CARDNUM\\\":0,\\\"ORGANCODE\\\":\\\"15\\\",\\\"CARDSN\\\":0}\"}",0)
		if err!=nil {
			println(err)
		}
	}
}

/*func TestPutKeyValue(t *testing.T) {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	kvapi:=&KVAPI{}
	kvstore:=kvapi.RaftInit(*id,*cluster,*join)
	var conf=make(chan<- raftpb.ConfChange)
	<-time.After(time.Second * 3)
	kvapi.store=kvstore
	kvapi.confChangeC=conf
	for i := 0; i < 100; i++ {
		T1:=time.Now()
		err:=kvapi.PUT("test00"+strconv.Itoa(i),"{\\\"ID\\\":0,\\\"USERID\\\":43097,\\\"ACCNO\\\":\\\"70636\\\",\\\"ACCTYPE\\\":0,\\\"CARDNO\\\":\\\"5330\\\",\\\"CARDID\\\":\\\"\\\",\\\"CARDBAL\\\":24968,\\\"BALANCE\\\":24968,\\\"QRYPASSWD\\\":\\\"1296DC193E9DBE33\\\",\\\"TXPASSWD\\\":\\\"1296DC193E9DBE33\\\",\\\"PASSCHKTOL\\\":0,\\\"TOTALDEPOSITE\\\":0,\\\"PLEDGECODE\\\":\\\"\\\",\\\"USECARDTIMES\\\":0,\\\"CDBALCHK\\\":\\\"\\\",\\\"BALCHK\\\":\\\"\\\",\\\"CARDSUBNUM\\\":0,\\\"DBSUBNUM\\\":0,\\\"ACCSTATUS\\\":\\\"1\\\",\\\"CMPACCSTS\\\":\\\"\\\",\\\"CMPACCTIMES\\\":0,\\\"INPUTUSERID\\\":0,\\\"PCODE\\\":\\\"\\\",\\\"OPENAMT\\\":0,\\\"CARDNUM\\\":0,\\\"ORGANCODE\\\":\\\"15\\\",\\\"CARDSN\\\":0}\"}")
		if err!=nil {
			println(err)
		}
		T2:=time.Now()
		println("Put耗时(纳秒):",strconv.Itoa(int(T2.Sub(T1).Nanoseconds())),",key:","test00"+strconv.Itoa(i))
	}
}*/

func TestGETKeyValue(t *testing.T) {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	zap.NewExample()
	kvapi:=&KVAPI{}
	kvstore:=kvapi.InitRaftCluster(*id,*cluster,*join)
	var conf=make(chan<- raftpb.ConfChange)
	// wait server started
	<-time.After(time.Second * 3)
	kvapi.Store=kvstore
	kvapi.ConfChangeC=conf
   var  err error
	/*valuebuf,err:=kvapi.Keys()
	println("valuebuf:",valuebuf)
	value,err:=kvapi.Get("test00zy0")
	if err!=nil{
		println(err.Error())
	}
	println("value:",value)*/
	for i := 0; i < 1; i++ {
		println("开始Put")
		T1:=time.Now()
		err:=kvapi.SyncPut("test00zy"+strconv.Itoa(i),"334324c4444",2*60*time.Second)
		if err!=nil{
			println(err.Error())
		}
		//value,err:=kvapi.Get("test00"+strconv.Itoa(i))
		/*if err!=nil {
			println(err,value)
		}*/
		T2:=time.Now()
		println("Put" +"耗时(纳秒):",strconv.Itoa(int(T2.Sub(T1).Nanoseconds())),",key:","test00zy"+strconv.Itoa(i))
	}
	//err=kvapi.Delete("test00zy0")
	if err!=nil{
		println(err.Error())
	}
	//value,_=kvapi.Get("test00zy0")
	//println("value:",value)
	time.Sleep(120*time.Second)
}


