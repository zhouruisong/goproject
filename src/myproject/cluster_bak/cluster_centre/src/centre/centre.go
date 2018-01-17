package centre

import (
	"fmt"
	"strings"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	log "github.com/Sirupsen/logrus"
)

type RetRequest struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
	Id     string  `json:"id"`
}
	
type ClusterMgr struct {
	Logger  *log.Logger
	MysqlAgent []string
	FdfsAgent  []string
	TairAgent  []string
}

func NewClusterMgr(mysqlagent []string, fdfsagent []string, tairagent []string, lg *log.Logger) *ClusterMgr {
	cl := &ClusterMgr{
		Logger:  lg,
		MysqlAgent: mysqlagent,
		FdfsAgent: fdfsagent,
		TairAgent: tairagent,
	}
	cl.Logger.Infof("NewClusterMgr ok")
	return cl
}

// 接收发送的文件消息，存入fastdfs，id写入tair
func (cl *ClusterMgr) UploadData(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ReadAll failed. %v", err)
	}
	
	var r RetRequest
	rt, id := cl.handlerUploadData(buf)
	if rt != 0 {
		r.Errno = rt
		r.Errmsg = "failed"
		r.Id = ""
	} else {
		r.Errno = rt
		r.Errmsg = "ok"
		r.Id = id
	}

	b, err := json.Marshal(&r)
	if err != nil {
		cl.Logger.Errorf("Marshal failed. %v", err)
	}

	res.Write(b) // HTTP 200
}

// 处理函数
func (cl *ClusterMgr) handlerUploadData(buf []byte) (int, string) {
	if len(buf) == 0 {
		cl.Logger.Errorf("buf len = 0")
		return -1, ""
	}
	
	url := fmt.Sprintf("http://%v/tair", cl.TairAgent[0])
	port := strings.Split(cl.TairAgent[0], ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", port[1])

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		cl.Logger.Errorf("http post return failed.err: %v , buf: %+v", err, string(buf))
		return -1, ""
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		cl.Logger.Errorf("ioutil readall failed.err:%v", err)
		return -1, ""
	}

	var ret RetRequest
	err = json.Unmarshal(result, &ret)
	if err != nil {
		cl.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return -1, ""
	}
	
	if (ret.Errno != 0) {
		return -1, ""
	}
//	cl.Logger.Infof("result return ok! url:%v, hosturl:%v, buf:%+v", url, hosturl, string(buf))
	cl.Logger.Infof("UploadAppenderByBuffer ok uploadres: %+v", ret.Id)
	return 0, ret.Id
}

// 接收发送的文件消息，存入fastdfs，id写入tair
//func (cl *ClusterMgr) DownloadData(res http.ResponseWriter, req *http.Request) {
//}
//}