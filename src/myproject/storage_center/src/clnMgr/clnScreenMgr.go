package clnMgr

import (
	_ "../common"
	"../devMgr"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	//"os"
	"sync"
	// "time"
)

type ClnScreenMgr struct {
	hosts     []string
	devmgr    *devMgr.DevScreenMgr
	devs      []devMgr.DevScreenInfo
	cleaners  []*Cleaner
	Logger    *log.Logger
	NotifyUrl string
}

type ItemsDate struct {
	Daytime string `json:"daytime"`
	Stream  string `json:"stream"`
}

type DetailDate struct {
	Count int         `json:"count"`
	Items []ItemsDate `json:"items"`
}

type DetailAddress struct {
	Count int    `json:"count"`
	Items string `json:"items"`
}

type DetailEntry struct {
	Count int      `json:"count"`
	Items []string `json:"items"`
}

//按天删除接口传入的参数
type InputDateInfo struct {
	Domain    string     `json:"domain"`
	NotifyUrl string     `json:"notify_url"`
	Dtype     int        `json:"dtype"`
	Details   DetailDate `json:"details"`
}

//按条删除接口传入的参数
type InputEntryInfo struct {
	Domain    string      `json:"domain"`
	NotifyUrl string      `json:"notify_url"`
	Dtype     int         `json:"dtype"`
	Details   DetailEntry `json:"details"`
}

//删除返回的信息结构
type DevRet struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewSrceenClnMgr(dm *devMgr.DevScreenMgr, lg *log.Logger) *ClnScreenMgr {
	dm.LoadScreenDevInfos()
	c := &ClnScreenMgr{
		devmgr: dm,
		devs:   dm.DevInfos,
		Logger: lg,
		// ch := make(chan DevTaskInfo, 100)
	}
	return c
}

// 透传消息到后台agent执行
func (clr *ClnScreenMgr) sendtodev(logid string, host string, buf []byte) (error, int) {
	body := bytes.NewBuffer([]byte(buf))
	url := fmt.Sprintf("http://%v/inquire_stream", host)
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.Logger.Errorf("logid:%v, http post return failed.err:%v", logid, err)
		return err, 2
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.Logger.Errorf("logid:%v, ioutil readall failed.err:%v", logid, err)
		return err, 2
	}

	var ret DevRet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		clr.Logger.Errorf("logid:%v, Error: cannot decode req body %v", logid, err)
		return err, 2
	}

	if ret.Code != 0 {
		clr.Logger.Errorf("logid:%v, input error :send id:%v, host%v, ret:%v, msg:%v", logid, host, ret.Code, ret.Message)
		return err, ret.Code
	}

	return nil, 0
}

//根据不同方式获取其中的notify_url
func (clr *ClnScreenMgr) getJsonInfo(mode int, logid string, buf []byte, pDevEntry *InputEntryInfo, pDevDate *InputDateInfo) int {
	if mode == 1 {
		var info InputEntryInfo
		err := json.Unmarshal(buf, &info)
		if err != nil {
			clr.Logger.Errorf("logid:%v, Error: cannot decode req body %v", logid, err)
			return -1
		}

		// if len(info.NotifyUrl) == 0 {
		// 	clr.Logger.Errorf("logid:%v, input error :notify_url:%v", logid, info.NotifyUrl)
		// 	return -1
		// }

		if len(info.Domain) == 0 {
			clr.Logger.Errorf("logid:%v, input error :domain:%v, notify_url:%v", logid, info.Domain, clr.NotifyUrl)
			return -1
		}

		pDevEntry.Domain = info.Domain
		pDevEntry.NotifyUrl = info.NotifyUrl
		pDevEntry.Dtype = info.Dtype
		pDevEntry.Details = info.Details
		clr.NotifyUrl = info.NotifyUrl
	} else if mode == 2 {
		var info InputDateInfo
		err := json.Unmarshal(buf, &info)
		if err != nil {
			clr.Logger.Errorf("logid:%v, Error: cannot decode req body %v", logid, err)
			return -1
		}

		// if len(info.NotifyUrl) == 0 {
		// 	clr.Logger.Errorf("logid:%v, input error :notify_url:%v", logid, info.NotifyUrl)
		// 	return -1
		// }

		if len(info.Domain) == 0 {
			clr.Logger.Errorf("logid:%v, input error :domain:%v, notify_url:%v", logid, info.Domain, clr.NotifyUrl)
			return -1
		}

		pDevDate.Domain = info.Domain
		pDevDate.NotifyUrl = info.NotifyUrl
		pDevDate.Dtype = info.Dtype
		pDevDate.Details = info.Details
		clr.NotifyUrl = info.NotifyUrl
	} else {
		clr.Logger.Errorf("logid:%v, invalid mode:%v", logid, mode)
		return -1
	}

	return 0
}

//从给定的文件信息做删除操作, mode 为1表示按条删除，为2表示按天删除
func (clr *ClnScreenMgr) CleanFile(mode int, logid string, buf []byte) error {
	clr.Logger.Infof("logid:%v, get req: %v", logid, string(buf))
	var results []DevRet

	var devEntry InputEntryInfo
	var devDate InputDateInfo

	if clr.getJsonInfo(mode, logid, buf, &devEntry, &devDate) < 0 {
		return nil
	}

	if mode == 1 {
		clr.Logger.Infof("logid:%v, zhouruisong rev clean file: %+v", logid, devEntry)
	} else if mode == 2 {
		clr.Logger.Infof("logid:%v, zhouruisong rev clean file: %+v", logid, devDate)
	}

	//分别向所有集群下发删除指令
	var waitgroup sync.WaitGroup
	for _, c := range clr.devs {
		waitgroup.Add(1)
		go clr.sendtoscreendev(logid, c.Host, buf, results)
	}

	// 等待2秒s
	// time.Sleep(2 * time.Second)
	waitgroup.Wait()

	for _, r := range results {
		if r.Code != 0 {
			if mode == 1 {
				clr.Logger.Errorf("logid:%v, mode:%v, clean %+v ok ", logid, mode, devEntry)
			} else if mode == 2 {
				clr.Logger.Errorf("logid:%v, mode:%v, clean %+v ok ", logid, mode, devDate)
			}
			return nil
		}
	}

	if mode == 1 {
		clr.Logger.Infof("logid:%v, clean %+v ok ", logid, devEntry)
	} else if mode == 2 {
		clr.Logger.Infof("logid:%v, clean %+v ok ", logid, devDate)
	}

	return nil
}

// 透传消息到后台agent执行
func (clr *ClnScreenMgr) sendtoscreendev(logid string, host string, buf []byte, results []DevRet) {
	body := bytes.NewBuffer([]byte(buf))
	url := fmt.Sprintf("http://%v/inquire_stream", host)
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.Logger.Errorf("logid:%v, host:%v, http post return failed.err:%v", logid, host, err)
		return
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.Logger.Errorf("logid:%v, host:%v, ioutil readall failed.err:%v", logid, host, err)
		return
	}

	var ret DevRet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		clr.Logger.Errorf("logid:%v, host:%v, Error: cannot decode req body %v", logid, host, err)
		return
	}

	if ret.Code != 0 || ret.Message != "ok" {
		clr.Logger.Errorf("logid:%v, input error :send id:%v, host%v, code:%v, msg:%v", logid, host, ret.Code, ret.Message)
		return
	}

	results = append(results, ret)
}
