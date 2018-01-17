package tair

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func (tair *TairClient) SendToBackUpTair(msg *protocal.SednTairPutBody) int {
	buff, err := json.Marshal(msg)
	if err != nil {
		tair.Logger.Errorf("Marshal failed err:%v, msg:%+v", err, msg)
		return -1
	}

	url := fmt.Sprintf("http://%v/tairreceive", tair.FdfsBackup)
	ip := strings.Split(tair.FdfsBackup, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buff))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		tair.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
		return -1
	}
	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		tair.Logger.Errorf("ioutil readall failed. err:%v", err)
		return -1
	}

	var ret protocal.MsgTairRet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		tair.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return -1
	}

	tair.Logger.Infof("ret:%+v", ret)
	return ret.Errno
}
