package fdfsmgr

import (
	"encoding/json"
//	"io/ioutil"
//	"net/http"
	"../protocal"
	"../fdfs_client"
	log "github.com/Sirupsen/logrus"
)

type FdfsMgr struct {
	pFdfs   *fdfs_client.FdfsClient
	Logger  *log.Logger
}

func NewClient(trackerlist []string, lg *log.Logger, minConns int, maxConns int) *FdfsMgr {
	pfdfs, err := fdfs_client.NewFdfsClient(trackerlist, lg, minConns, maxConns)
	if err != nil {
		lg.Errorf("NewClient failed")
		return nil
	}
	
	fd := &FdfsMgr{
		pFdfs:   pfdfs,
		Logger:  lg,
	}
	fd.Logger.Infof("NewClient ok")
	return fd
}

func (fdfs *FdfsMgr) HandlerDownloadFile(logid string, buf []byte) (int, []byte) {
	var ret_buf []byte
	var q protocal.CentreDownloadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		fdfs.Logger.Errorf("Unmarshal error:%v", err)
		return -1, ret_buf
	}
	
	fdfs.Logger.Infof("before DownloadToBuffer logid:%+v, id:%+v", 
		logid, q.Id)
	
	downloadResponse, err := fdfs.pFdfs.DownloadToBuffer(q.Id, 0, 0)
	if err != nil {
		fdfs.Logger.Errorf("DownloadToBuffer fail, logid:%+v, err:%v, id:%+v", 
			err, logid, q.Id)
		return -1, ret_buf
	}

	if value, ok := downloadResponse.Content.([]byte); ok {
		fdfs.Logger.Infof("DownloadToBuffer ok logid:%+v, id:%+v", 
			logid, q.Id)
		return 0, value
	}

	return -1, ret_buf
}

// 处理函数
func (fdfs *FdfsMgr) HandlerUploadFile(logid string, buf []byte) (int, string) {
	if len(buf) == 0 {
		fdfs.Logger.Errorf("handlerUploadFile buf len = 0")
		return -1, ""
	}

	var q protocal.CentreUploadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		fdfs.Logger.Errorf("Error: cannot decode err:%v",err)
		return -1, ""
	}

	fdfs.Logger.Infof("before UploadAppenderByBuffer logid:%+v, file:%+v", 
		logid, q.Filename)

	uploadres, err := fdfs.pFdfs.UploadAppenderByBuffer(q.Content, "")
	if err != nil {
		fdfs.Logger.Errorf("UploadAppenderByBuffer failed err:%v, logid:%+v, file:%+v", 
			err, logid, q.Filename)
		return -1, ""
	}

	fdfs.Logger.Infof("UploadAppenderByBuffer ok uploadres:%+v, logid:%+v, file:%+v", 
		uploadres, logid, q.Filename)
	return 0, uploadres.RemoteFileId
}
