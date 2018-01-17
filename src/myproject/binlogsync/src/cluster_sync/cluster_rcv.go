package cluster_sync

import (
	"../protocal"
	"encoding/json"
//	"fmt"
	"io/ioutil"
	"net/http"
)

// 接收上传机发送的db消息
func (sy *SyncMgr) Login(res http.ResponseWriter, req *http.Request) {
//	req.ParseForm()
//	fmt.Println(req)
	sy.Logger.Infof("zhouruisong")
	var ret protocal.RetCentreUploadFile
	ret.Errno = 0
	ret.Errmsg = "success"
	ret.Id = "1"
	b, _ := json.Marshal(ret)
	res.Write(b)
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

	sy.Logger.Infof("return: %+v", ret)
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

	sy.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收上传机发送的db消息
func (sy *SyncMgr) UploadPut(res http.ResponseWriter, req *http.Request) {
	var rt int
	var msg string
	var b []byte
	var err_marshal error
	var ret protocal.MsgMysqlRet

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

	rt, msg = sy.handleUploadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		ret.Errno = -1
		ret.Errmsg = "Marshal failed"
		goto END
	}

	if ret.Errno != 0 {
		sy.Logger.Infof("return: %+v", ret)
	}

END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (sy *SyncMgr) handleUploadData(buf []byte) (int, string) {
	var data protocal.MsgMysqlBody
	err := json.Unmarshal(buf, &data)
	if err != nil {
		sy.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, "Unmarshal failed"
	}

	//sy.Logger.Infof("data: %+v", data)
	// 更新成功收到的Id，重启服务时可以指定是否大于这个id增量上传
	if sy.SaveLastSuccessIdToFile(data.Data.ID, data.TableName) != 0 {
		sy.Logger.Errorf("SaveLastSuccessIdToFile failed taskid: %+v, tablename: %+v",
			data.Data.TaskID, data.TableName)
		return -1, "SaveLastSuccessIdToFile failed"
	}

	//data.Status == 0 表示是源数据处理完毕，需要同步， 为1表示是备份数据，不需要同步
	if data.Data.IsBackup == 0 {
		// 指定时间同步，需要先写文件
		if sy.SyncPartTime {
			//			sy.SaveFailedTofile(fmt.Sprintf("http://%s/%s/%s", "127.0.0.1",
			//				data.Data.Domain, data.Data.FileName))
		} else {
			if sy.pBin.Write(&data) != 0 {
				sy.Logger.Errorf("write channel failed: taskid: %+v", data.Data.TaskID)
				return -1, "write channel failed"
			}
		}
	} else {
		sy.Logger.Errorf("data.Data.IsBackup = %+v", data.Data.IsBackup)
	}

	sy.Logger.Infof("get upload msg successful, taskid: %+v", data.Data.TaskID)
	return 0, "ok"
}
