package cluster_sync

import (
	"../protocal"
	"encoding/json"
	//	"fmt"
	"io/ioutil"
	"net/http"
)

// 接收发送的文件消息，存入fastdfs
func (sy *SyncMgr) FileUpload(res http.ResponseWriter, req *http.Request) {
	var rt int
	var msg string
	var b []byte
	var err_marshal error
	var ret protocal.RetUploadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		sy.Logger.Errorf("ReadAll failed. err:%v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sy.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, msg = sy.HandlerUploadFilename(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sy.Logger.Errorf("Marshal failed. err:%v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	sy.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收发送的文件消息，存入fastdfs
func (sy *SyncMgr) FastdfsPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var id string
	var msg string
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreUploadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		sy.Logger.Errorf("ReadAll failed. err:%v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sy.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, id, msg = sy.pTran.HandlerUploadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
		ret.Id = id
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sy.Logger.Errorf("Marshal failed. err:%v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		ret.Id = ""
		goto END
	}

	//sy.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收发送的文件消息，存入fastdfs
func (sy *SyncMgr) FastdfsGetData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var msg string
	var content []byte
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreDownloadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		sy.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sy.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, content, msg = sy.pTran.HandlerDownloadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
		ret.Content = content
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sy.Logger.Errorf("Marshal failed. %v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	sy.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收发送的文件消息，存入fastdfs
func (sy *SyncMgr) FastdfsDeleteData(res http.ResponseWriter, req *http.Request) {
	var rt error
	var b []byte
	var msg string
	var err_marshal error
	var ret protocal.RetCentreDeleteFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		sy.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sy.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, msg = sy.pTran.HandlerDeleteData(buf)
	if rt != nil {
		ret.Errno = -1
		ret.Errmsg = msg
	} else {
		ret.Errno = 0
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sy.Logger.Errorf("Marshal failed. %v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	sy.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收DB同步过来的内容，插入对应的live_master表中
func (sy *SyncMgr) MysqlReceive(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		sy.Logger.Errorf("ReadAll failed. %v", err)
	}

	var ret protocal.MsgMysqlRet
	rt, msg := sy.pSql.Handlerdbinsert(buf)

	if rt == 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err := json.Marshal(ret)
	if err != nil {
		sy.Logger.Errorf("Marshal failed. %v", err)
	}

	sy.Logger.Infof("return: %+v", ret)
	res.Write(b) // HTTP 200
}

// 接收发送过来的上传tair消息，将id存入tair
func (sy *SyncMgr) TairReceive(res http.ResponseWriter, req *http.Request) {
	var rt int
	var b []byte
	var msg string
	var err_marshal error
	var ret protocal.MsgTairRet

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		sy.Logger.Errorf("ReadAll failed err:%v", err)
		ret.Errno = -1
		ret.Errmsg = "Readall failed"
		goto END
	}
	if len(buf) == 0 {
		ret.Errno = -1
		ret.Errmsg = "buf len = 0"
		goto END
	}

	rt, msg = sy.pTran.PTair.HandlerSendtoTairPut(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sy.Logger.Errorf("Marshal failed err:%v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "marshal failed"
		goto END
	}

	//sy.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}
