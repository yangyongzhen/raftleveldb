package raftleveldb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)
var cli = http.DefaultClient
type HTTPClient struct{
	Endpoints []string `json:"endpoints"`
	DialTimeout time.Duration `json:"dial-timeout"`
	//上下文超时时间
	OpTimeout   time.Duration `json:"op_timeout"`

}

func (httpClient *HTTPClient)Put(key, value string) (string, error) {
	url := fmt.Sprintf("%s/%s", httpClient.Endpoints[0], key)
	body := bytes.NewBufferString(value)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return "",err
	}
	req.Header.Set("Content-Type", "text/html; charset=utf-8")
	_, err = cli.Do(req)
	if err != nil {
		return "",err
	}
	return "",nil
}

func (httpClient *HTTPClient)Get(key string) (string, error) {
	    url := fmt.Sprintf("%s/%s", httpClient.Endpoints[0], key)
		resp, err := cli.Get(url)
		if err != nil {
			return "",err
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "",err
		}
		defer resp.Body.Close()
		if string(data)=="Failed to GET\n"{
			return "",fmt.Errorf(string(data))
		}
		return string(data) ,nil
}




