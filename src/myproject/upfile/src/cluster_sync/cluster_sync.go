package cluster_sync

import (
	"../binlogmgr"
	"../mysqlmgr"
	"../protocal"
	"../transfer"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	//"strconv"
	"time"
)

type SyncMgr struct {
	pBin     *binlogmgr.BinLogMgr
	pTran    *transfer.TransferMgr
	pSql     *mysqlmgr.MysqlMgr
	FdfsCh   chan *protocal.Ctx
	ReqCh    chan *protocal.UploadFile
	Logger   *log.Logger
	Cacheip  string
}

func NewSyncMgr(my *binlogmgr.BinLogMgr, tran *transfer.TransferMgr,
	sql *mysqlmgr.MysqlMgr, num int, cacheip string, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pBin:    my,
		pTran:   tran,
		pSql:    sql,
		FdfsCh:  make(chan *protocal.Ctx, 2048),
		ReqCh:   make(chan *protocal.UploadFile, 2048),
		Logger:  lg,
		Cacheip: cacheip,
	}

	for i := 0; i < num; i++ {
		go sy.cunsumeFdfsCh()
	}

	for i := 0; i < (num/10); i++ {
		go sy.cunsumeReqCh()
	}

	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sy *SyncMgr) Read() *protocal.UploadFile {
	info, isClose := <-sy.ReqCh
	if !isClose {
		sy.Logger.Infof("channel closed!")
		return nil
	}
	return info
}

func (sy *SyncMgr) Write(info *protocal.UploadFile) int {
	select {
	case sy.ReqCh <- info:
		return 0
	case <-time.After(time.Second * 10):
		sy.Logger.Infof("write to channel timeout: %+v", info)
		return -1
	}
	return 0
}

func (sy *SyncMgr) cunsumeReqCh() {
	for {
		q := sy.Read()
		if q == nil {
			break
		}

		ret, message := sy.SplitFileUpload(q)
		//上传成功，给上传机创建任务，从cache下载源文件
		if ret == 0 {
			source_url := fmt.Sprintf("http://%v/%s%s", sy.Cacheip, q.Domain, q.Uri)
			sy.Logger.Infof("source_url:%+v, taskid:%+v", source_url, q.Taskid)
			ret, message = sy.pTran.FileTaskAddNew(q, source_url)
			sy.Logger.Infof("taskid:%v,FileTaskAddNew code:%v,message:%v",q.Taskid,ret,message)
		}else{
			sy.Logger.Errorf("SplitFileUpload taskid:%v,ret:%v", q.Taskid,ret)
		}

		//回调用户结果
		if sy.pTran.SendNotify(ret, message, q.Taskid) != 0 {
			sy.Logger.Errorf("SendNotify failed,taskid:%v", q.Taskid)
		}
	}
	return
}

func (sy *SyncMgr) HandlerUploadFilename(buf []byte) (int, string) {
	var q protocal.UploadFile
	err := json.Unmarshal(buf, &q)
	if err != nil {
		sy.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, "HandlerUploadData Unmarshal failed"
	}
	
	if sy.Write(&q) != 0 {
		sy.Logger.Errorf("write channel failed: taskid: %+v", q.Taskid)
		return -1, "write channel failed"
	}
	return 0, "success"
}

func (sy *SyncMgr) process(buf []byte, taskid string, subTaskid string, slice int,flag bool) string {
	return sy.pTran.Sendbuff(buf, taskid, subTaskid, slice,flag)
}

func (sy *SyncMgr) getFileFromFdfs(id string) (int, []byte) {
	var retry int
	var retbuff []byte

	for retry := 0; retry < 10; retry++ {
		// get file from fdfs
		rt, fileBuff := sy.pTran.PFdfs.HandlerDownloadFile(id)
		if rt == 0 && len(fileBuff) > 0 {
			retbuff = fileBuff
			break
		}
	}

	if retry >= 10 {
		sy.Logger.Errorf("get data from fdfs failed,sliceid:%+v", id)
		return -1, retbuff
	}

	sy.Logger.Infof("get data from fdfs ok,sliceid:%+v", id)
	return 0, retbuff
}

func (sy *SyncMgr) putIndexFile(data *protocal.UploadFile, id string) int {
	current_time := fmt.Sprintf("%v", time.Now().Unix())
	keys := protocal.SendTairPut{
		Prefix:     data.Domain,
		Key:        data.Uri + ".index",
		Value:      id,
		CreateTime: current_time,
		ExpireTime: current_time,
	}

	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)
	sy.Logger.Infof("ready put index into tair,id:%+v, %+v", id, msg)

	return sy.pTran.PTair.SendToBackUpTair(&msg)
}

func (sy *SyncMgr) getIndexFile(prefix string, key string) *protocal.RetTairGet {
	keys := protocal.SendTairGet{
		Prefix: prefix,
		Key:    "/" + prefix + key + ".index",
	}

	var msg protocal.SednTairGetBody
	msg.Keys = append(msg.Keys, keys)

	buf, err := json.Marshal(msg)
	if err != nil {
		sy.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return nil
	}

	var ret protocal.RetTairGet
	ret.Errno, ret.Keys = sy.pTran.PTair.HandlerSendtoTairGet(buf)

	return &ret
}

func (sy *SyncMgr) getIndexFileFromFdfs(pData *protocal.DbInfo) (int, []byte) {
	// first get index id from tair
	var ret_byte []byte
	ret := sy.getIndexFile(pData.Domain, pData.URI)
	if ret.Errno != 0 {
		sy.Logger.Errorf("ret: %+v", ret)
		return -1, ret_byte
	}

	// get index file from fdfs
	rt, fileBuff := sy.pTran.PFdfs.HandlerDownloadFile(ret.Keys[0].Value)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ret_byte
	}

	return 0, fileBuff
}
