package tair

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"bytes"
	"strings"
	"encoding/json"
	"../domainmgr"
	log "github.com/Sirupsen/logrus"
)

type DetailGet struct {
	Prefix string `json:"prefix"`
	Key    string `json:"key"`
}

type DetailPut struct {
	Prefix     string `json:"prefix"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	CreateTime uint64 `json:"createtime"`
	ExpireTime uint64 `json:"expiretime"`
}

type MsgGetBody struct {
	Command    string      `json:"command"`
	ServerAddr string      `json:"server_addr"`
	GroupName  string      `json:"group_name"`
	Keys       []DetailGet `json:"keys"`
}

type MsgPutBody struct {
	Command    string      `json:"command"`
	ServerAddr string      `json:"server_addr"`
	GroupName  string      `json:"group_name"`
	Keys       []DetailPut `json:"keys"`
}

type RetTairGet struct {
	Keys []DetailPut `json:"keys"`
}

type RetTairPut struct {
	Keys []DetailPut `json:"keys"`
}

type TairClient struct {
	Logger     *log.Logger
	ServerAddr   string
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
		ServerAddr:   sever_addr,
		Tairclient: tairclient,
	}
	c.Logger.Infof("NewTairClient ok")
	return c
}

// 向tair上传
func (tair *TairClient) SendtoTairPut(pMsg *domainmgr.StreamInfo, id string) error {
	msg := MsgPutBody{
		Command:    "pput",
		ServerAddr: tair.ServerAddr,
		GroupName:  "group_1",
	}

	detail := DetailPut{
		Prefix:     pMsg.Domain,
		Key:        pMsg.FileName,
		Value:      id,
		CreateTime: pMsg.PublishTime,
		ExpireTime: 7776000,
	}

	msg.Keys = append(msg.Keys, detail)

	tair.Logger.Infof("Keys:%v", msg.Keys)

	url := fmt.Sprintf("http://%v/tair", tair.Tairclient)

	port := strings.Split(tair.Tairclient, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", port[1])

	buf, err := json.Marshal(msg)
	if err != nil {
		tair.Logger.Errorf("Marshal failed.err:%v, buf: %+v", err, string(buf))
		return err
	}

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		tair.Logger.Errorf("http post return failed.err: %v , buf: %+v", err, string(buf))
		return err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		tair.Logger.Errorf("ioutil readall failed.err:%v", err)
		return err
	}

	var ret RetTairPut
	err = json.Unmarshal(result, &ret)
	if err != nil {
		tair.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return err
	}

	tair.Logger.Infof("result return ok! url:%v, hosturl:%v, buf:%+v", url, hosturl, string(buf))
	return nil
}

// 向tair获取value
func (tair *TairClient) SendtoTairGet(pMsg *domainmgr.StreamInfo) ([]string, error) {
	msg := MsgGetBody{
		Command:    "pget",
		ServerAddr: tair.ServerAddr,
		GroupName:  "group_1",
	}

	var ids []string
	//key := fmt.Sprintf("\\//%+v", pMsg.FileName)
	// for _, info := range pMsg.StreamInfos {
	// detail := DetailGet{
	// 	Prefix:     pMsg.Domain,
	// 	Key:         pMsg.FileName,
	// }

	detail := DetailGet{
		Prefix: "test.com",
		Key:    "1",
	}

	msg.Keys = append(msg.Keys, detail)
	// }

	url := fmt.Sprintf("http://%v/tair", tair.Tairclient)
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", tair.Tairclient)

	buf, err := json.Marshal(msg)
	if err != nil {
		tair.Logger.Errorf("Marshal failed.err:%v, buf: %+v", err, string(buf))
		return ids, err
	}

	body := bytes.NewBuffer([]byte(buf))

	res, err := http.Post(url, hosturl, body)
	if err != nil {
		tair.Logger.Errorf("url:%v, buf: %+v, res:%+v", url, string(buf), res)
		return ids, err
	}

	tair.Logger.Infof("Post ok url:%v, buf: %+v, res:%+v", url, string(buf), res)
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		tair.Logger.Errorf("host:%v, ioutil readall failed.err:%v", tair.Tairclient, err)
		return ids, err
	}

	var ret RetTairGet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		tair.Logger.Errorf("host:%v, cannot decode req body Error, result:%v", tair.Tairclient, err)
		return ids, err
	}

	tair.Logger.Infof("result return ok! url:%v, hosturl:%v, ret:%+v", url, hosturl, ret)

	for _, id := range ret.Keys {
		ids = append(ids, id.Value)
	}

	return ids, nil
}
