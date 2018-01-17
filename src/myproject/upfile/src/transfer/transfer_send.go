package transfer

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func (cl *TransferMgr) FileTaskAddNew(q *protocal.UploadFile, source_url string) (int, string) {
	url := fmt.Sprintf("%v?Action=LiveMaintain.FileTaskAddNew", cl.UpMachine)
	msg := fmt.Sprintf("&taskid=%s&domain=%s&behavior=%s&fname=%s&ftype=%s&url=%s&cb_url=%s",
		q.Taskid, q.Domain, q.Behavior, q.Fname, q.Ftype, source_url, q.CbUrl)

	url += msg
	//cl.Logger.Infof("taskid: %+v,url: %v", q.Taskid, url)

	res, err := http.Get(url)
	if err != nil {
		cl.Logger.Errorf("http post return failed,taskid:%+v,err:%v", q.Taskid, err)
		return 1, "FileTaskAddNew failed"
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed,taskid:%+v,err:%v,result:%+v", q.Taskid, err, string(result))
		return 1, "FileTaskAddNew failed"
	}

	var ret protocal.RetTaskAdd
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode req body Error,taskid:%+v,err:%v,result:%+v", q.Taskid, err, string(result))
		return 1, "FileTaskAddNew failed"
	}

	//cl.Logger.Infof("taskid:%+v,ret: %+v", q.Taskid, ret)

	code, _ := strconv.Atoi(ret.Errno)
	if code == 200 {
		return 0, ret.Errmsg
	}
	return code, ret.Errmsg
}

//提交表单
func (cl *TransferMgr) SendNotify(code int, message string, taskid string) int {
	//这里添加post的body内容
	data := make(url.Values)
	data["code"] = []string{strconv.Itoa(code)}
	data["message"] = []string{message}
	data["task_id"] = []string{taskid}

	url := fmt.Sprintf("http://%v/task_result", cl.CallBack)

	//cl.Logger.Infof("url:%+v", url)
	//cl.Logger.Infof("data:%+v", data)

	//把post表单发送给目标服务器
	res, err := http.PostForm(url, data)
	if err != nil {
		cl.Logger.Errorf("PostForm failed. err:%v", err)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed. err:%v", err)
		return -1
	}

	var ret protocal.RetCbUploadFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode result body err:%+v", err)
		return -1
	}

	//cl.Logger.Infof("ret:%+v", ret)
	return ret.Errno
}

func (cl *TransferMgr) Sendbuff(fileBuffer []byte, taskid string, subTaskid string, slice int, flag bool) string {
	var res *http.Response
	retry := 0
	for ; retry < 3; retry++ {
		msg := protocal.CentreUploadFile{
			IndexFlag: 0,
			Taskid:    taskid,
			SubTaskid: subTaskid,
			Sliceid:   slice,
			Content:   fileBuffer,
		}

		if flag == true {
			msg.IndexFlag = 1
		}

		buf, err := json.Marshal(msg)
		if err != nil {
			cl.Logger.Errorf("Marshal failed,taskid:%v,subTaskid:%v,sliceid:%v,err:%v", taskid, subTaskid, slice, err)
			return ""
		}

		url := fmt.Sprintf("http://%v/fdfsput", cl.UpServer)

		ip := strings.Split(cl.UpServer, ":")
		hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

		body := bytes.NewBuffer([]byte(buf))

		res1, err := http.Post(url, hosturl, body)
		if err != nil {
			cl.Logger.Errorf("post failed,taskid:%v,subTaskid:%v,sliceid:%v,err:%+v",
				taskid, subTaskid, slice, err)
		} else {
			res = res1
			break
		}
	}

	if retry >= 3 || res == nil {
		cl.Logger.Errorf("Sendbuff failed,taskid:%v,subTaskid:%v,sliceid:%v",
			taskid, subTaskid, slice)
		return ""
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed,taskid:%v,subTaskid:%v,sliceid:%v,err:%+v",
			taskid, subTaskid, slice, err)
		return ""
	}

	var ret protocal.RetCentreUploadFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode req body,taskid:%v,subTaskid:%v,sliceid:%v,err:%+v",
			taskid, subTaskid, slice, err)
		return ""
	}

	cl.Logger.Infof("taskid:%v,sliceid:%v,ret:%+v", taskid, slice, ret)
	return ret.Id
}

func (cl *TransferMgr) Downloadbuff(id string) (int, []byte) {
	var ret_buff []byte
	msg := protocal.CentreDownloadFile{
		Id: id,
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		cl.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return -1, ret_buff
	}

	url := fmt.Sprintf("http://%v/fdfsget", cl.UpServer)
	ip := strings.Split(cl.UpServer, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		cl.Logger.Errorf("http post return failed.err: %v , buf: %+v", err, buf)
		return -1, ret_buff
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed.err:%v", err)
		return -1, ret_buff
	}

	var ret protocal.RetCentreDownloadFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return -1, ret_buff
	}

	cl.Logger.Infof("downloadbuff ret.Errno: %+v", ret.Errno)
	return 0, ret.Content
}

func (cl *TransferMgr) Deletebuff(taskid string, subTaskid string, sliceid int, id string) int {
	var res *http.Response
	retry := 0
	for ; retry < 3; retry++ {
		msg := protocal.CentreDeleteFile{
			Taskid:    taskid,
			SubTaskid: subTaskid,
			Sliceid:   sliceid,
			Id:        id,
		}

		buf, err := json.Marshal(msg)
		if err != nil {
			cl.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
			return -1
		}

		url := fmt.Sprintf("http://%v/fdfsdelete", cl.UpServer)
		ip := strings.Split(cl.UpServer, ":")
		hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

		body := bytes.NewBuffer([]byte(buf))
		res1, err := http.Post(url, hosturl, body)
		if err != nil {
			cl.Logger.Errorf("http post return failed,taskid:%v,subTaskid:%v,sliceid:%v,err:%+v",
				taskid, subTaskid, sliceid, err)
		} else {
			res = res1
			break
		}
	}

	defer res.Body.Close()
	if retry >= 3 || res == nil {
		cl.Logger.Errorf("failed,taskid:%v,subTaskid:%v,sliceid:%v",
			taskid, subTaskid, sliceid)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed.err:%v", err)
		return -1
	}

	var ret protocal.RetCentreDeleteFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return -1
	}

	cl.Logger.Infof("ret:%+v,taskid:%v,sliceid:%v,id:%v,", ret, taskid, sliceid, id)
	return ret.Errno
}
