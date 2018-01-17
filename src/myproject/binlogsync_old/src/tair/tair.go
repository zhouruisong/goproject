package tair

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
)

type TairClient struct {
	Logger     *log.Logger
	TairServer string
	Tairclient string
}

func NewTairClient(server []string, tairclient string, lg *log.Logger) *TairClient {
	var sever_addr string
	if len(server) == 2 {
		sever_addr = server[0] + "," + server[1]
	} else if len(server) == 1 {
		sever_addr = server[0]
	} else {
		fmt.Println("ERROR: tair_server len: %d", len(server))
		return nil
	}

	c := &TairClient{
		Logger:     lg,
		TairServer: sever_addr,
		Tairclient: tairclient,
	}
	c.Logger.Infof("NewTairClient ok")
	return c
}

func (tair *TairClient) HandlerSendtoTairPut(buf []byte) (int, string) {
	var q protocal.SednTairPutBody
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tair.Logger.Errorf("Unmarshal error:%v", err)
		return -1, ""
	}

	msg := protocal.SendTairMesage{
		Command:    "pput",
		ServerAddr: tair.TairServer,
		GroupName:  "group_1",
		Keys:       q.Keys,
	}

	buff, err := json.Marshal(msg)
	if err != nil {
		tair.Logger.Errorf("Marshal failed err:%v, msg:%+v", err, msg)
		return -1, ""
	}

	url := fmt.Sprintf("http://%v/tair", tair.Tairclient)
	ip := strings.Split(tair.Tairclient, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buff))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		tair.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
		return -1, ""
	}

	defer res.Body.Close()

	if res.StatusCode == 200 {
		tair.Logger.Infof("tairput return ok code:%+v, status:%+v", res.StatusCode, res.Status)
		return 0, res.Status
	}

	tair.Logger.Infof("tairput return failed code:%+v, status:%+v", res.StatusCode, res.Status)
	return -1, ""
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
		tair.Logger.Infof("tairget return ok code:%+v, status:%+v", res.StatusCode, res.Status)
		return 0, RetKeys.Keys
	}

	tair.Logger.Infof("tairget return failed, code:%+v, status:%+v", res.StatusCode, res.Status)
	return -1, ret_buff
}
