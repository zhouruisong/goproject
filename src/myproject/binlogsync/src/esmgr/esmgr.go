package esmgr

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	//	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v3"
	"io/ioutil"
	"net/http"
	"strings"
)

type EsMgr struct {
	Logger   *log.Logger
	Client   *elastic.Client
	EsServer string
}

func NewEsMgr(server []string, lg *log.Logger) *EsMgr {
	var sever_addr string
	if len(server) == 2 {
		sever_addr = server[0] + "," + server[1]
	} else if len(server) == 1 {
		sever_addr = server[0]
	} else {
		fmt.Println("ERROR: es_server len: %d", len(server))
		return nil
	}

	client, err := elastic.NewClient(elastic.SetURL(sever_addr))
	//client, err := elastic.NewClient(elastic.SetURL(sever_addr), elastic.SetBasicAuth("user", "secret"))
	if err != nil {
		fmt.Println("ERROR: NewClient failed %+v", err)
		return nil
	}

	e := &EsMgr{
		Logger:   lg,
		Client:   client,
		EsServer: sever_addr,
	}

	e.Logger.Infof("NewEsMgr ok")

	//e.CreateIndex("www.wasu.cn")
	//
	//	e.CheckExistInEs("wasu")

	return e
}

func (es *EsMgr) CreateIndex(name string) bool {
	// Create an index
	client := es.Client

	// Add a document to the index
	tweet := `{
	    "properties":{
            "task_id":{
                "type":"string"
            },
            "action":{
                "type":"int"
            },
            "domain":{
                "type":"string"
            },
            "filename":{
                "type":"string"
            },
            "file_size":{
                "type":"int"
            },
            "create_time":{
                "type":"date"
            },
           "ff_uri":{
                "type":"string"
            }
	    }
    }`

	createService, err := client.CreateIndex(name).Body(tweet).Do()
	if err != nil {
		es.Logger.Infof("Index:%v has exist", name)
		return true
	}

	if createService == nil {
		es.Logger.Errorf("CreateIndex failed")
		return false
	}

	es.Logger.Infof("CreateIndex ok")
	return true
}

func (es *EsMgr) HandlerSendToEs(q *protocal.SendEsBody) int {
	client := es.Client
	var es_type string
	if q.Action == 0 {
		es_type = "UP"
	} else {
		es_type = "DEL"
	}

	es.Logger.Infof("q: %+v", q)

	// Add a document
	_, err := client.Index().
		Index(q.Domain).
		Type(es_type).
		Id("").
		BodyJson(q).
		Refresh(true).Do()

	if err != nil {
		es.Logger.Errorf("Add a document failed err:%+v", err)
		return -1
	}

	//flush
	_, err = client.Flush().Index(q.Domain).Do()
	if err != nil {
		es.Logger.Errorf("Flush a document failed err:%+v", err)
		return -1
	}

	// Count documents
	count, err := client.Count(q.Domain).Do()
	if err != nil {
		es.Logger.Errorf("Count documents failed err:%+v", err)
		return -1
	}

	es.Logger.Infof("Domain: %+v,total count:%+v", q.Domain, count)

	// Count documents
	count, err = client.Count(q.Domain).Type("UP").Do()
	if err != nil {
		es.Logger.Errorf("Count documents failed err:%+v", err)
		return -1
	}

	es.Logger.Infof("Domain: %+v,type UP count:%+v", q.Domain, count)

	// Count documents
	count, err = client.Count(q.Domain).Type("DEL").Do()
	if err != nil {
		es.Logger.Errorf("Count documents failed err:%+v", err)
		return -1
	}

	es.Logger.Infof("Domain :%+v,type DEL count:%+v", q.Domain, count)

	es.Logger.Infof("HandlerSendToEs ok: %+v", q)

	return 0
}

func (es *EsMgr) HandlerSendToEso(q *protocal.SendEsBody) int {
	buff, err := json.Marshal(q)
	if err != nil {
		es.Logger.Errorf("Marshal failed err:%v, q:%+v", err, q)
		return -1
	}

	url := fmt.Sprintf("%v/%s/%s/", es.EsServer, q.Domain, "UP")
	ip := strings.Split(es.EsServer, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])
	//	es.Logger.Infof("url:%+v", url)

	body := bytes.NewBuffer([]byte(buff))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		es.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
		return -1
	}

	defer res.Body.Close()
	//	if err != nil {
	//		es.Logger.Errorf("ioutil readall failed, err:%v, buff:%+v", err, string(buff))
	//		return -1
	//	}

	//	es.Logger.Infof("res:%+v", result)
	//	var RetKeys protocal.RetTairGetKeys
	//	err = json.Unmarshal(result, &RetKeys)
	//	if err != nil {
	//		tair.Logger.Errorf("Unmarshal return body error, err:%v, buff:%+v", err, string(buff))
	//		return -1, ret_buff
	//	}

	if res.StatusCode == 201 && res.Status == "201 Created" {
		es.Logger.Infof("return ok code:%+v, status:%+v",
			res.StatusCode, res.Status)
		return 0
	}

	es.Logger.Infof("return failed code:%+v, status:%+v",
		res.StatusCode, res.Status)
	return -1
}

//统计用户下的文件个数
func (es *EsMgr) GetDomainNumber(q *protocal.GetEsInput) int {
	url := fmt.Sprintf("http://%v/%s/_search?search_type=count",
		es.EsServer, q.Domain)

	es.Logger.Infof("url:%+v", url)

	res, err := http.Get(url)
	if err != nil {
		es.Logger.Errorf("http get return failed.err:%v , q:%+v", err, q)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		es.Logger.Errorf("ioutil readall failed, err:%v, q:%+v", err, q)
		return -1
	}

	var Ret protocal.RetGetEsBody
	err = json.Unmarshal(result, &Ret)
	if err != nil {
		es.Logger.Errorf("Unmarshal return body error, err:%v, q:%+v", err, q)
		return -1
	}

	es.Logger.Infof("GetDomainNumber:%+v", Ret)

	return Ret.Hit.Total
}

//检查文件是否存在
func (es *EsMgr) CheckExistInEs(name string) bool {
	indexExists, err := es.Client.IndexExists(name).Do()
	if err != nil {
		es.Logger.Errorf("IndexExists failed. err:%+v, name:%+v", err, name)
		return false
	}

	if !indexExists {
		es.Logger.Errorf("expected index exists=%v; got %v", true, indexExists)
		return false
	}

	es.Logger.Infof("Index name:%v exist", name)

	return indexExists
}

//检查文件是否存在
//func (es *EsMgr) CheckExistInEso(q *protocal.GetEsInput) bool {
//	url := fmt.Sprintf("http://%v/%s/_search/exists?q=filename:\"%s\"",
//		es.EsServer, q.Domain, q.FileName)
//
//	res, err := http.Get(url)
//	if err != nil {
//		es.Logger.Errorf("http get return failed.err:%v , q:%+v", err, q)
//		return false
//	}
//
//	result, err := ioutil.ReadAll(res.Body)
//	defer res.Body.Close()
//	if err != nil {
//		es.Logger.Errorf("ioutil readall failed, err:%v, q:%+v", err, q)
//		return false
//	}
//
//	var Ret protocal.RetCheckExist
//	err = json.Unmarshal(result, &Ret)
//	if err != nil {
//		es.Logger.Errorf("Unmarshal return body error, err:%v, q:%+v", err, q)
//		return false
//	}
//
//	es.Logger.Infof("CheckExistInEs:%+v", Ret)
//
//	return Ret.Exists
//}

//获取指定用户下的指定文件
func (es *EsMgr) HandlerGetFromEs(q *protocal.GetEsInput) int {
	url := fmt.Sprintf("http://%v/%s/_search?q=filename:\"%s\"",
		es.EsServer, q.Domain, q.FileName)
	//	es.Logger.Infof("url:%+v", url)

	res, err := http.Get(url)
	if err != nil {
		es.Logger.Errorf("http get return failed.err:%v , q:%+v", err, q)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		es.Logger.Errorf("ioutil readall failed, err:%v, q:%+v", err, q)
		return -1
	}

	var RetGetEs protocal.RetGetEsBody
	err = json.Unmarshal(result, &RetGetEs)
	if err != nil {
		es.Logger.Errorf("Unmarshal return body error, err:%v, q:%+v", err, q)
		return -1
	}

	es.Logger.Infof("RetGetEs:%+v", RetGetEs)
	if res.StatusCode == 200 {
		es.Logger.Infof("return ok code:%+v, status:%+v",
			res.StatusCode, res.Status)
		return 0
	}

	es.Logger.Infof("return failed code:%+v, status:%+v",
		res.StatusCode, res.Status)
	return -1
}
