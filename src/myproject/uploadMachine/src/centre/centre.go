package centre

import (
	"fmt"
//	"strings"
//	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"../fdfsmgr"
	"../protocal"
	"../tair"
	log "github.com/Sirupsen/logrus"
	uuid "github.com/satori/go.uuid"
)
	
type ClusterMgr struct {
	Logger  *log.Logger
	pFdfs *fdfsmgr.FdfsMgr
	pTair *tair.TairClient
	MysqlAgent []string
	FdfsAgent  []string
	TairAgent  []string
}

func NewClusterMgr(pfdfs *fdfsmgr.FdfsMgr , ptair *tair.TairClient, mysqlagent []string, 
	fdfsagent []string, tairagent []string, lg *log.Logger) *ClusterMgr {
	cl := &ClusterMgr{
		Logger:  lg,
		pFdfs: pfdfs,
		pTair: ptair,
		MysqlAgent: mysqlagent,
		FdfsAgent: fdfsagent,
		TairAgent: tairagent,
	}
	cl.Logger.Infof("NewClusterMgr ok")
	return cl
}

// 接收发送的文件消息，存入fastdfs，id写入tair
func (cl *ClusterMgr) FastdfsPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var id string
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreUploadFile
	logid := fmt.Sprintf("%s", uuid.NewV4())
	
	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
//	defer req.Close()
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

	rt, id = cl.handlerUploadData(logid, buf)
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
	
	cl.Logger.Infof("logid:%+v, FastdfsPutData return ret:%+v", logid, string(b))
END:	
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *ClusterMgr) handlerUploadData(logid string, buf []byte) (int, string) {	
	var q protocal.CentreUploadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		cl.Logger.Errorf("Unmarshal error logid:%+v, err:%v", logid, err)
		return -1, ""
	}
	
	result, id := cl.pFdfs.HandlerUploadFile(logid, buf)	
	if result != 0 {
		cl.Logger.Errorf("Unmarshal return body error, logid:%+v, err:%v, file:%+v", 
			logid, err, q.Filename)
		return -1, ""
	}
	
	//cl.Logger.Infof("logid:%+v, handlerUploadData return ret:%+v, file:%+v", 
	//	logid, result, q.Filename)
	return 0, id
}

// 接收发送的文件消息，存入fastdfs，id写入tair
func (cl *ClusterMgr) FastdfsGetData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var content []byte
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreDownloadFile
	logid := fmt.Sprintf("%s", uuid.NewV4())
	
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

	rt, content = cl.handlerDownloadData(logid, buf)
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
//	
	cl.Logger.Infof("logid:%+v, FastdfsGetData return ret: %+v", logid, ret.Errno)
END:	
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *ClusterMgr) handlerDownloadData(logid string, buf []byte) (int, []byte) {	
	var ret_buf []byte
	result, content := cl.pFdfs.HandlerDownloadFile(logid, buf)	
	if result != 0 {
		cl.Logger.Errorf("logid:%+v,result:%+v,len:%+v", logid, result, len(content))
		return -1, ret_buf
	}
	
//	cl.Logger.Infof("logid:%+v, handlerUploadData ok result: %+v", logid, result)
	return 0, content
}

// 接收发送的id写入tair
func (cl *ClusterMgr) TairPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var b []byte
	var err_marshal error
	var ret protocal.RetTairPut
	logid := fmt.Sprintf("%s", uuid.NewV4())
	
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

	rt = cl.handlerSendToTairPut(logid, buf)
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
	
	cl.Logger.Infof("logid:%+v, TairPutData return ret:%+v", logid, string(b))
END:	
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *ClusterMgr) handlerSendToTairPut(logid string, buf []byte) int {	
	ret, _ := cl.pTair.HandlerSendtoTairPut(buf)
	//cl.Logger.Infof("logid:%+v, handlerSendToTairPut return ret:%+v", logid, ret)
	return ret
}

// 接收发送的id写入tair
func (cl *ClusterMgr) TairGetData(res http.ResponseWriter, req *http.Request) {
	var b []byte
	var err_marshal error
	var ret protocal.RetTairGet
	logid := fmt.Sprintf("%s", uuid.NewV4())
	
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

	cl.handlerSendToTairGet(logid, buf, &ret)
	if ret.Errno == 0 {
		ret.Errmsg = "ok"
	}
	
	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		cl.Logger.Errorf("Marshal failed. %v", err_marshal)
		return
	}
	
	cl.Logger.Infof("logid:%+v, TairGetData return  ret:%+v", logid, string(b))
END:	
	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *ClusterMgr) handlerSendToTairGet(logid string, buf []byte, ret *protocal.RetTairGet) {
	ret.Errno, ret.Keys = cl.pTair.HandlerSendtoTairGet(buf)
	//cl.Logger.Infof("handlerSendToTairGet return logid:%+v, ret:%+v", logid, ret)
	return
}