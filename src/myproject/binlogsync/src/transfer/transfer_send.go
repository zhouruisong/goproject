package transfer

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func (cl *TransferMgr) Sendbuff(fileBuffer []byte, taskid string) string {
	msg := protocal.CentreUploadFile{
		Taskid:  taskid,
		Content: fileBuffer,
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		cl.Logger.Errorf("Marshal failed.err:%v, taskid: %+v", err, msg.Taskid)
		return ""
	}

	url := fmt.Sprintf("http://%v/fdfsput", cl.FdfsBackup)

	ip := strings.Split(cl.FdfsBackup, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		cl.Logger.Errorf("http post return failed. err: %v , taskid: %+v", err, msg.Taskid)
		return ""
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed. err:%v", err)
		return ""
	}

	var ret protocal.RetCentreUploadFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return ""
	}

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

	url := fmt.Sprintf("http://%v/fdfsget", cl.FdfsBackup)
	ip := strings.Split(cl.FdfsBackup, ":")
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

func (cl *TransferMgr) Deletebuff(id string) int {
	msg := protocal.CentreDeleteFile{
		Id: id,
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		cl.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return -1
	}

	url := fmt.Sprintf("http://%v/fdfsdelete", cl.FdfsBackup)
	ip := strings.Split(cl.FdfsBackup, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		cl.Logger.Errorf("http post return failed.err: %v , buf: %+v", err, buf)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
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

	cl.Logger.Infof("downloadbuff ret.Errno: %+v", ret.Errno)
	return ret.Errno
}
