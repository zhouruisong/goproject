package fdfsmgr

import (
	"../fdfs_client"
	"../protocal"
	//	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
)

type FdfsMgr struct {
	pFdfs     *fdfs_client.FdfsClient
	Logger    *log.Logger
	CacheList []string
}

// 接收上传机发送的db消息
func (fdfs *FdfsMgr) UploadFileToGroup(res http.ResponseWriter, req *http.Request) {
	var rt int
	var msg string
	var b []byte
	var err_marshal error
	var ret protocal.UploadFileRet

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		ret.Errno = -1
		ret.Errmsg = "ReadAll failed"
		goto END
	}
	if len(buf) == 0 {
		ret.Errno = -1
		ret.Errmsg = "buf len = 0"
		goto END
	}

	rt, msg = fdfs.UploadToGroup(buf)
	ret.Errno = rt
	ret.Errmsg = msg

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		ret.Errno = -1
		ret.Errmsg = "Marshal failed"
		goto END
	}

END:
	res.Write(b) // HTTP 200
}

func NewClient(trackerlist []string, cache []string, lg *log.Logger, minConns int, maxConns int) *FdfsMgr {
	pfdfs, err := fdfs_client.NewFdfsClient(trackerlist, lg, minConns, maxConns)
	if err != nil {
		lg.Errorf("NewClient failed")
		return nil
	}

	fd := &FdfsMgr{
		pFdfs:     pfdfs,
		Logger:    lg,
		CacheList: cache,
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
func (fdfs *FdfsMgr) UploadToGroup(buf []byte) (int, string) {
	if len(buf) == 0 {
		fdfs.Logger.Errorf("buf len = 0")
		return -1, "buf len = 0"
	}

	var input protocal.GroupInfos
	err := json.Unmarshal(buf, &input)
	if err != nil {
		fdfs.Logger.Errorf("Unmarshal error:%v", err)
		return -1, "Unmarshal error"
	}

	for _, q := range input.Groupdetail {
		iplist := strings.Replace(q.Iplist, " ", "", -1)
		fdfs.Logger.Infof("iplist: %v", iplist)

		storageIps := strings.Split(iplist, ",")

		//循环向统一个group中的所有storage上传buff
		for _, ip := range storageIps {
			for i := 0; i < q.TotalIndex; i++ {
				content := fmt.Sprintf("%s:%s:%d:%d", q.GroupName, ip, q.Port, i)
				contentbuff := []byte(content)
				fdfs.Logger.Infof("content: %+v", content)
				//上传数据到指定ip的storage
				uploadres, err := fdfs.pFdfs.UploadByBufferToGroup(contentbuff, "", ip, q.Port,
					q.GroupName, i)
				if err != nil {
					uploaderr := fmt.Sprintf("Upload test failed storage:%v index:%d err:%v", ip, i, err)
					fdfs.Logger.Errorf("%v", uploaderr)
					return -1, uploaderr
				}

				ret_groupname := strings.Split(uploadres.RemoteFileId, "/")
				//group 不相等
				if q.GroupName != ret_groupname[0] {
					errinfo := fmt.Sprintf("storage:%v is belong to %v, not belong to %v",
						ip, ret_groupname[0], q.GroupName)
					fdfs.Logger.Errorf("%v", errinfo)
					return -1, errinfo
				}

				fdfs.Logger.Infof("Upload %v ok,id[%v]", content, uploadres.RemoteFileId)

				//循环从cache读取上传的数据，进行比较，不想等或者网络问题，返回错误
				for _, cacheip := range fdfs.CacheList {
					ret, content_read := fdfs.readFromStorage(cacheip, uploadres.RemoteFileId)
					//网络错误直接返回
					if ret != 0 {
						retinfo := fmt.Sprintf("cache:%s read id:%v from storage:%v index:%d error: %v, please check config",
							cacheip, uploadres.RemoteFileId, ip, i, content_read)

						fdfs.Logger.Errorf("%v", retinfo)
						return ret, retinfo
					}

					//进行内容比较，不想等也返回
					if ret == 0 && content != content_read {
						retinfo := fmt.Sprintf("%s read %v info:%v from storage:%v index:%v is not equal %s, please check config",
							cacheip, uploadres.RemoteFileId, content_read, ip, i, content)

						fdfs.Logger.Errorf("%v", retinfo)
						return -1, retinfo
					}

					fdfs.Logger.Infof("cache:%s read %v from storage:%v index:%d ok",
						cacheip, uploadres.RemoteFileId, ip, i)
				}

				//删除上传的文件
				err = fdfs.pFdfs.DeleteFile(uploadres.RemoteFileId)
				if err != nil {
					delerr := fmt.Sprintf("DeleteFile file %v failed storage:%v index:%d err:%v",
						uploadres.RemoteFileId, ip, i, err)
					fdfs.Logger.Errorf("%v", delerr)
					return -1, delerr
				}

				fdfs.Logger.Infof("DeleteFile file[%v] ok", uploadres.RemoteFileId)
			}
		}
	}

	info := fmt.Sprintf("Upload test success")
	fdfs.Logger.Infof("%s", info)
	return 0, info
}

// 处理函数
func (fdfs *FdfsMgr) readFromStorage(ip, id string) (int, string) {
	url := fmt.Sprintf("http://%v:8088/%s", ip, id)
	fdfs.Logger.Infof("http post url: curl %+v", url)

	res, err := http.Get(url)
	if err != nil {
		fdfs.Logger.Errorf("http post return failed:%+v", err)
		return -1, "http get return failed"
	}

	defer res.Body.Close()
	result, err := ioutil.ReadAll(res.Body)

	if err != nil {
		fdfs.Logger.Errorf("ioutil readall failed, err:%v", err)
		return -1, "ioutil readall failed"
	}

	fdfs.Logger.Infof("result: %+v", string(result))
	return 0, string(result)
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
