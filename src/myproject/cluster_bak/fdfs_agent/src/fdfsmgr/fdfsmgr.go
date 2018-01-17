package fdfsmgr

import (
	"../fdfs_client"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

var (
	id uint32
	number int
)

type StreamInfo struct {
	Id           uint32
	TaskId       string
	TaskServer   string
	FileName     string
	FileType     uint8
	FileSize     uint32
	FileMd5      string
	Domain       string
	App          string
	Stream       string
	Step         uint8
	PublishTime  uint64
	NotifyUrl    string
	NotifyReturn string
	Status       uint8
	ExpireTime   string
	CreateTime   string
	UpdateTime   string
	EndTime      string
	NotifyTime   string
}

type RetSendStream struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
	Id     string  `json:"id"`
}

type RetSendBuff struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

type MsgStreamBody struct {
	TableName string    `json:"tablename"`
	Data []StreamInfo    `json:"data"`
}

// filetype=1 m3u8, filetype=0 ts
type MsgSendBuff struct {
	FileType int      `json:"filetype"`
	Domain   string   `json:"domain"`
	FileName string  `json:"filename"`
	App       string   `json:"app"`
	PublishTime uint64   `json:"publishtime"`
	Content  []byte   `json:"content"`
}

type FdfsMgr struct {
	pFdfs   *fdfs_client.FdfsClient
	Logger  *log.Logger
}

func NewClient(trackerlist []string, lg *log.Logger, minConns int, maxConns int) *FdfsMgr {
	pfdfs, err := fdfs_client.NewFdfsClient(trackerlist, lg, minConns, maxConns)
	if err == nil {
		return nil
	}
	
	fd := &FdfsMgr{
		pFdfs:   pfdfs,
		Logger:  lg,
	}
	fd.Logger.Infof("NewSyncMgr ok")
	return fd
}

func (fdfs *FdfsMgr) DownloadFileToBuffer(id string) ([]byte, error) {
	downloadResponse, err := fdfs.pFdfs.DownloadToBuffer(id, 0, 0)
	if err != nil {
		fdfs.Logger.Errorf("DownloadToBuffer fail: %+v", id)
		return nil, err
	}
	fdfs.Logger.Infof("downloadResponse: %+v", downloadResponse)

	var buf []byte
	if value, ok := downloadResponse.Content.([]byte); ok {
		return value, nil
	}

	return buf, nil
}

// 接收发送的文件消息，存入fastdfs，id写入tair
func (fdfs *FdfsMgr) UploadFile(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		fdfs.Logger.Errorf("ReadAll failed. %v", err)
	}

	rt, id := fdfs.handlerbuff(buf)
	r := RetSendStream{
		Errno:  rt,
		Errmsg: "ok",
		Id: id,
	}

	b, err := json.Marshal(&r)
	if err != nil {
		fdfs.Logger.Errorf("Marshal failed. %v", err)
	}

	res.Write(b) // HTTP 200
}

// 处理函数
func (fdfs *FdfsMgr) handlerbuff(buf []byte) (int, string) {
	if len(buf) == 0 {
		fdfs.Logger.Errorf("buf len = 0")
		return -1, ""
	}

	var q MsgSendBuff
	err := json.Unmarshal(buf, &q)
	if err != nil {
		fdfs.Logger.Errorf("Error: cannot decode req body %v", err)
		return -1, ""
	}

	fdfs.Logger.Infof("handlerdbinfo q: %+v", q)

	uploadres, err := fdfs.pFdfs.UploadAppenderByBuffer(q.Content, q.FileName)
	if err != nil {
		fdfs.Logger.Errorf("UploadAppenderByBuffer failed err: %v", err)
		return -1, ""
	}

	fdfs.Logger.Infof("UploadAppenderByBuffer ok uploadres: %+v", uploadres)
	return 0, uploadres.RemoteFileId
}
