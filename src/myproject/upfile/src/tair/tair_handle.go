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

func (tair *TairClient) HandlerSendtoTairPut(buf []byte) (int, string) {
	var q protocal.SednTairPutBody
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tair.Logger.Errorf("Unmarshal error:%v", err)
		return -1, "Unmarshal failed"
	}

	msg := protocal.SendTairMesage{
		Command:    "pput",
		ServerAddr: tair.TairServer,
		GroupName:  "group_1",
		Keys:       q.Keys,
	}
	
	//tair.Logger.Infof("tairput: %+v", msg)
	
	buff, err := json.Marshal(msg)
	if err != nil {
		tair.Logger.Errorf("Marshal failed err:%v, msg:%+v", err, msg)
		return -1, "TairPut Marshal failed"
	}

	url := fmt.Sprintf("http://%v/tair", tair.Tairclient)
	ip := strings.Split(tair.Tairclient, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buff))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		tair.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
		return -1, "TairPut http post return failed"
	}

	defer res.Body.Close()

	if res.StatusCode == 200 {
		tair.Logger.Infof("tairput return code:%+v,status:%+v,%+v",res.StatusCode,res.Status,msg)
		return 0, "ok"
	}

	tair.Logger.Infof("tairput return failed code:%+v,status:%+v,%+v",res.StatusCode,res.Status,msg)
	return -1, "TairPut failed"
}

// 向tair获取value
func (tair *TairClient) HandlerSendtoTairGet(buf []byte) (int, []protocal.RetTairGetDetail) {
	var ret_buff []protocal.RetTairGetDetail
	var q protocal.SednTairGetBody
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tair.Logger.Errorf("Unmarshal error:%v", err)
		return -1, ret_buff
	}

	msg := protocal.SendTairMesageGet{
		Command:    "pget",
		ServerAddr: tair.TairServer,
		GroupName:  "group_1",
		Keys:       q.Keys,
	}

	buff, err := json.Marshal(msg)
	if err != nil {
		tair.Logger.Errorf("Marshal failed.err:%v, msg:%+v", err, msg)
		return -1, ret_buff
	}

	url := fmt.Sprintf("http://%v/tair", tair.Tairclient)
	ip := strings.Split(tair.Tairclient, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buff))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		tair.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
		return -1, ret_buff
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		tair.Logger.Errorf("ioutil readall failed, err:%v, buff:%+v", err, string(buff))
		return -1, ret_buff
	}

	var RetKeys protocal.RetTairGetKeys
	err = json.Unmarshal(result, &RetKeys)
	if err != nil {
		tair.Logger.Errorf("Unmarshal return body error, err:%v, buff:%+v", err, string(buff))
		return -1, ret_buff
	}

	if res.StatusCode == 200 {
		//tair.Logger.Infof("return ok code:%+v, status:%+v", res.StatusCode, res.Status)
		return 0, RetKeys.Keys
	}

	tair.Logger.Infof("return failed, code:%+v, status:%+v", res.StatusCode, res.Status)
	return -1, ret_buff
}
