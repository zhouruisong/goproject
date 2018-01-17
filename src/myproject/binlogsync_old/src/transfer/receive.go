package transfer

import (
	"../fdfsmgr"
	"../protocal"
	"../tair"
	"encoding/json"
	//	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

type TransferMgr struct {
	Logger     *log.Logger
	PFdfs      *fdfsmgr.FdfsMgr
	PTair      *tair.TairClient
	FdfsBackup string
}

func NewTransferMgr(pfdfs *fdfsmgr.FdfsMgr, ptair *tair.TairClient, peerip string, lg *log.Logger) *TransferMgr {
	cl := &TransferMgr{
		Logger:     lg,
		PFdfs:      pfdfs,
		PTair:      ptair,
		FdfsBackup: peerip,
	}
	cl.Logger.Infof("NewClusterMgr ok")
	return cl
}

// 接收发送的文件消息，存入fastdfs
func (cl *TransferMgr) FastdfsPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var id string
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreUploadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		cl.Logger.Errorf("ReadAll failed. err:%v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		cl.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, id = cl.handlerUploadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = "failed"
	} else {
		ret.Errno = rt
		ret.Errmsg = "ok"
		ret.Id = id
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		cl.Logger.Errorf("Marshal failed. err:%v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		ret.Id = ""
		goto END
	}

	cl.Logger.Infof("FastdfsPutData return ret:%+v", string(b))
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *TransferMgr) handlerUploadData(buf []byte) (int, string) {
	var q protocal.CentreUploadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		cl.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, ""
	}

	result, id := cl.PFdfs.HandlerUploadFile(q.Content)
	if result != 0 {
		cl.Logger.Errorf("HandlerUploadFile error, Taskid:%+v", q.Taskid)
		return -1, ""
	}

	return 0, id
}

// 接收发送的文件消息，存入fastdfs
func (cl *TransferMgr) FastdfsGetData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var content []byte
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreDownloadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		cl.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, content = cl.handlerDownloadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = "failed"
	} else {
		ret.Errno = rt
		ret.Errmsg = "ok"
		ret.Content = content
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		cl.Logger.Errorf("Marshal failed. %v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	cl.Logger.Infof("FastdfsGetData return ret: %+v", ret.Errno)
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *TransferMgr) handlerDownloadData(buf []byte) (int, []byte) {
	var ret_buf []byte
	var q protocal.CentreDownloadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		cl.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, ret_buf
	}

	var result int
	result, ret_buf = cl.PFdfs.HandlerDownloadFile(q.Id)
	if result != 0 {
		cl.Logger.Errorf("result:%+v", result)
		return -1, ret_buf
	}

	cl.Logger.Infof("handlerUploadData ok result: %+v", result)
	return 0, ret_buf
}

// 接收发送的文件消息，存入fastdfs
func (cl *TransferMgr) FastdfsDeleteData(res http.ResponseWriter, req *http.Request) {
	var rt error
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreDeleteFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		cl.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt = cl.handlerDeleteData(buf)
	if rt != nil {
		ret.Errno = 1
		ret.Errmsg = "failed"
	} else {
		ret.Errno = 0
		ret.Errmsg = "ok"
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		cl.Logger.Errorf("Marshal failed. %v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	cl.Logger.Infof("FastdfsGetData return ret: %+v", ret.Errno)
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *TransferMgr) handlerDeleteData(buf []byte) error {
	var q protocal.CentreDeleteFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		cl.Logger.Errorf("Unmarshal error err:%v", err)
		return err
	}

	err = cl.PFdfs.HandlerDeleteFile(q.Id)
	if err != nil {
		cl.Logger.Errorf("HandlerDeleteFile error err:%+v", err)
		return err
	}

	cl.Logger.Infof("handlerDeleteData ok id: %+v", q.Id)
	return nil
}

// 接收发送的id写入tair
func (cl *TransferMgr) TairPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var b []byte
	var err_marshal error
	var ret protocal.RetTairPut

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		cl.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt = cl.handlerSendToTairPut(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = "failed"
	} else {
		ret.Errno = rt
		ret.Errmsg = "ok"
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		cl.Logger.Errorf("Marshal failed. %v", err_marshal)
		return
	}

	cl.Logger.Infof("TairPutData return ret:%+v", string(b))
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *TransferMgr) handlerSendToTairPut(buf []byte) int {
	ret, _ := cl.PTair.HandlerSendtoTairPut(buf)
	cl.Logger.Infof("handlerSendToTairPut return ret:%+v", ret)
	return ret
}

// 接收发送的id写入tair
func (cl *TransferMgr) TairGetData(res http.ResponseWriter, req *http.Request) {
	var b []byte
	var err_marshal error
	var ret protocal.RetTairGet

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		cl.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	cl.handlerSendToTairGet(buf, &ret)
	if ret.Errno == 0 {
		ret.Errmsg = "ok"
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		cl.Logger.Errorf("Marshal failed. %v", err_marshal)
		return
	}

	cl.Logger.Infof("TairGetData return  ret:%+v", string(b))
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *TransferMgr) handlerSendToTairGet(buf []byte, ret *protocal.RetTairGet) {
	ret.Errno, ret.Keys = cl.PTair.HandlerSendtoTairGet(buf)
	cl.Logger.Infof("handlerSendToTairGet return ret:%+v", ret)
	return
}
