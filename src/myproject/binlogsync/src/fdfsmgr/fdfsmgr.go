package fdfsmgr

import (
	"../fdfs_client"
	log "github.com/Sirupsen/logrus"
)

type FdfsMgr struct {
	pFdfs  *fdfs_client.FdfsClient
	Logger *log.Logger
}

func NewClient(trackerlist []string, lg *log.Logger, minConns int, maxConns int) *FdfsMgr {
	pfdfs, err := fdfs_client.NewFdfsClient(trackerlist, lg, minConns, maxConns)
	if err != nil {
		lg.Errorf("NewClient failed")
		return nil
	}

	fd := &FdfsMgr{
		pFdfs:  pfdfs,
		Logger: lg,
	}
	fd.Logger.Infof("NewClient ok")
	return fd
}

func (fdfs *FdfsMgr) HandlerDeleteFile(id string) error {
	err := fdfs.pFdfs.DeleteFile(id)
	if err != nil {
		fdfs.Logger.Errorf("fail, err:%v, id:%+v", err, id)
		return err
	}

	return nil
}

func (fdfs *FdfsMgr) HandlerDownloadFile(id string) (int, []byte) {
	var ret_buf []byte
	downloadResponse, err := fdfs.pFdfs.DownloadToBuffer(id, 0, 0)
	if err != nil {
		fdfs.Logger.Errorf("fail, err:%v, id:%+v",
			err, id)
		return -1, ret_buf
	}

	if value, ok := downloadResponse.Content.([]byte); ok {
		fdfs.Logger.Infof("ok id: %+v", id)
		return 0, value
	}

	return -1, ret_buf
}

// 处理函数
func (fdfs *FdfsMgr) HandlerUploadFile(buf []byte) (int, string) {
	if len(buf) == 0 {
		fdfs.Logger.Errorf("buf len = 0")
		return -1, ""
	}

	uploadres, err := fdfs.pFdfs.UploadAppenderByBuffer(buf, "")
	if err != nil {
		fdfs.Logger.Errorf("UploadAppenderByBuffer failed err:%v", err)
		return -1, ""
	}

	fdfs.Logger.Infof("ok uploadres:%+v", uploadres)
	return 0, uploadres.RemoteFileId
}
