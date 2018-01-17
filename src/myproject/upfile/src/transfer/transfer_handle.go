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
	Logger    *log.Logger
	PFdfs     *fdfsmgr.FdfsMgr
	PTair     *tair.TairClient
	UpServer  string
	UpMachine string
	CallBack  string
}

func NewTransferMgr(pfdfs *fdfsmgr.FdfsMgr, ptair *tair.TairClient, upsrv string, upmachine string, callback string,lg *log.Logger) *TransferMgr {
	tran := &TransferMgr{
		Logger:    lg,
		PFdfs:     pfdfs,
		PTair:     ptair,
		UpServer:  upsrv,
		UpMachine: upmachine,
		CallBack:  callback,
	}
	tran.Logger.Infof("NewClusterMgr ok")
	return tran
}

// 处理函数
func (tran *TransferMgr) HandlerUploadData(buf []byte) (int, string, string) {
	var q protocal.CentreUploadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tran.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, "", "HandlerUploadData Unmarshal failed"
	}

	var fdfsId string
	retry := 0
	for ; retry < 10; retry++ {
		result, id := tran.PFdfs.HandlerUploadFile(q.Content)
		if result != 0 {
			tran.Logger.Errorf("HandlerUploadFile failed,taskid:%+v,sliceid:%+v", q.Taskid,q.Sliceid)
		} else {
			fdfsId = id
			break
		}
	}

	if retry >= 10 {
		tran.Logger.Errorf("HandlerUploadFile failed,taskid:%+v,sliceid:%+v", q.Taskid,q.Sliceid)
		return -1, "", "HandlerUploadFile failed"
	}

	//index upload ok
	if q.IndexFlag == 1 {
		tran.Logger.Infof("upload index ok,taskid:%+v,sliceid:%+v,id:%+v", q.Taskid,q.Sliceid,fdfsId)
	}else{
		tran.Logger.Infof("taskid:%+v,sliceid:%+v,id:%+v", q.Taskid,q.Sliceid,fdfsId)
	}

	return 0, fdfsId, "ok"
}

// 处理函数
func (tran *TransferMgr) HandlerUploadFilename(buf []byte) (int, string, string) {
	var q protocal.UploadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tran.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, "", "HandlerUploadData Unmarshal failed"
	}

	result, id := tran.PFdfs.HandlerUploadByFilename(q.Fpath)
	if result != 0 {
		tran.Logger.Errorf("error, Taskid:%+v", q.Taskid)
		return -1, "", "HandlerUploadFile failed"
	}

	tran.Logger.Infof("return taskid: %+v, id: %+v", q.Taskid, id)
	return 0, id, "ok"
}

// 处理函数
func (tran *TransferMgr) HandlerDownloadData(buf []byte) (int, []byte, string) {
	var ret_buf []byte
	var q protocal.CentreDownloadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tran.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, ret_buf, "HandlerDownloadData Unmarshal failed"
	}

	var result int
	result, ret_buf = tran.PFdfs.HandlerDownloadFile(q.Id)
	if result != 0 {
		tran.Logger.Errorf("result:%+v", result)
		return -1, ret_buf, "HandlerDownloadFile failed"
	}

	tran.Logger.Infof("handlerUploadData ok result: %+v", result)
	return 0, ret_buf, "ok"
}

// 处理函数
func (tran *TransferMgr) HandlerDeleteData(buf []byte) (error, string) {
	var q protocal.CentreDeleteFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		tran.Logger.Errorf("Unmarshal error err:%v", err)
		return err, "HandlerDeleteData Unmarshal failed"
	}

	err = tran.PFdfs.HandlerDeleteFile(q.Id)
	if err != nil {
		tran.Logger.Errorf("error,taskid:%+v,sliceid:%+v,id:%+v,err:%+v", 
			q.Taskid,q.Sliceid,q.Id,err)
		return err, "HandlerDeleteFile failed"
	}

	tran.Logger.Infof("ok,taskid:%+v,sliceid:%+v,id:%+v", q.Taskid,q.Sliceid,q.Id)
	return nil, "ok"
}

// 接收发送的id写入tair
func (tran *TransferMgr) TairPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var b []byte
	var msg string
	var err_marshal error
	var ret protocal.RetTairPut

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		tran.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		tran.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, msg = tran.HandlerSendToTairPut(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		tran.Logger.Errorf("Marshal failed. %v", err_marshal)
		return
	}

	tran.Logger.Infof("TairPutData return ret:%+v", string(b))
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (tran *TransferMgr) HandlerSendToTairPut(buf []byte) (int, string) {
	return tran.PTair.HandlerSendtoTairPut(buf)
}

// 接收发送的id写入tair
func (tran *TransferMgr) TairGetData(res http.ResponseWriter, req *http.Request) {
	var b []byte
	var err_marshal error
	var ret protocal.RetTairGet

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		tran.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		tran.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	tran.HandlerSendToTairGet(buf, &ret)
	if ret.Errno == 0 {
		ret.Errmsg = "ok"
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		tran.Logger.Errorf("Marshal failed. %v", err_marshal)
		return
	}

	tran.Logger.Infof("TairGetData return  ret:%+v", string(b))
END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (tran *TransferMgr) HandlerSendToTairGet(buf []byte, ret *protocal.RetTairGet) {
	ret.Errno, ret.Keys = tran.PTair.HandlerSendtoTairGet(buf)
	tran.Logger.Infof("handlerSendToTairGet return ret:%+v", ret)
	return
}
