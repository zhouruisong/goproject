package cluster_sync

import (
	"../binlogmgr"
	"../esmgr"
	"../indexmgr"
	"../mysqlmgr"
	"../protocal"
	"../transfer"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	//	"math/rand"
	//	"strconv"
	"strings"
	"sync"
)

var openSync = 0
var restartFlag = 0

type SyncMgr struct {
	pBin         *binlogmgr.BinLogMgr
	pTran        *transfer.TransferMgr
	pSql         *mysqlmgr.MysqlMgr
	pEs          *esmgr.EsMgr
	Logger       *log.Logger
	pFdfsCh      chan protocal.Ctx
	pResCh       chan int
	Interval     int
	SyncPartTime bool
	UploadServer string
}

func NewSyncMgr(inval int, server string, my *binlogmgr.BinLogMgr, tran *transfer.TransferMgr,
	sql *mysqlmgr.MysqlMgr, es *esmgr.EsMgr, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pBin:         my,
		pTran:        tran,
		pSql:         sql,
		pEs:          es,
		Logger:       lg,
		Interval:     inval,
		pFdfsCh:      make(chan protocal.Ctx, 1000),
		pResCh:       make(chan int),
		SyncPartTime: my.GetSyncPartTime(),
		UploadServer: server,
	}

	if sy.InitCache() != nil {
		return nil
	}

	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sy *SyncMgr) RunDateMigrate() {
	sy.Logger.Infof("start IncreaseSync.")

	// 程序重启时，需要从db获取一段时间内积累的数据同步到备份集群
	if restartFlag == 1 {
		go sy.SendOldSyncTask()
	}

	// 程序启动时，启动定时器处理失败的任务
	go sy.TimerSendFailedTask()

	// 读取上传机发送过来的db数据
	go sy.readIncreaseInfo()

	return
}

func (sy *SyncMgr) HanlePipeCache() {

}

func (sy *SyncMgr) SetFlag(flag, reflag int) {
	openSync = flag
	restartFlag = reflag
}

func (sy *SyncMgr) SendUploadServer(msg *protocal.UploadInfo) (int, error) {
	fmtinfo := "http://%v/index.php?Action=LiveMaintain.FileTaskAddNew&taskid=%s" +
		"&domain=%s&behavior=%s&fname=%s&ftype=%s&url=%s&cb_url=%s&md5_type=%d"
	url := fmt.Sprintf(fmtinfo,
		sy.UploadServer, msg.TaskId, msg.Domain, msg.Behavior, msg.FileName,
		msg.FileType, msg.Url, msg.CbUrl, msg.Md5Type)

	ip := strings.Split(sy.UploadServer, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	sy.Logger.Infof("url: %+v", url)

	body := bytes.NewBuffer([]byte(""))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		sy.Logger.Errorf("http post return failed.err:%v", err)
		return -1, err
	}

	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		sy.Logger.Errorf("ioutil readall failed, err:%v", err)
		return -1, err
	}

	var ret protocal.RetUploadMeg
	err = json.Unmarshal(result, &ret)
	if err != nil {
		sy.Logger.Errorf("Unmarshal return body error, err:%v", err)
		return -1, err
	}

	sy.Logger.Infof("ret: %+v", ret)

	// 成功
	if ret.Code == 200 {
		return 0, nil
	}

	// 任务已经存在，删除失败表中的该任务，避免重复上传
	if ret.Code == 2 {
		return 2, nil
	}

	return -1, nil
}

func (sy *SyncMgr) sendBuff(i int, indexcache *protocal.IndexCache, data *protocal.MsgMysqlBody) int {
	r, buf := sy.getFileFromFdfs(indexcache.Item[i].Id)
	if r == 0 {
		// 将二级索引中每一片内容上传到备份的fdfs中
		id := sy.pTran.Sendbuff(buf, data.Data.TaskID)
		if id != "" {
			// 将返回的id存储到备份集群的tair中
			ret := sy.putIndexFile(data, id)
			if ret != 0 {
				// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
				rt := sy.pTran.Deletebuff(id)
				if rt != 0 {
					sy.Logger.Errorf("delete data from standby fdfs failed,id:%+v", data.Data.ID)
					return -1
				}
			}
			// 更新二级索引中的id，换成备份集群的id
			indexcache.Item[i].Id = id
		} else {
			sy.Logger.Errorf("put data to standby fdfs failed,id: %+v", data.Data.ID)
			return -1
		}
	} else {
		sy.Logger.Errorf(" get data to master fdfs failed,id: %+v", data.Data.ID)
		return -1
	}

	return 1
}

func (sy *SyncMgr) process(i int, indexcache *protocal.IndexCache, data *protocal.MsgMysqlBody) int {
	// 根据二级索引中的id，从本集群获取对应的内容
	r, buf := sy.getFileFromFdfs(indexcache.Item[i].Id)
	if r == 0 {
		// 将二级索引中每一片内容上传到备份的fdfs中
		id := sy.pTran.Sendbuff(buf, data.Data.TaskID)
		if id != "" {
			// 将返回的id存储到备份集群的tair中
			ret := sy.putIndexFile(data, id)
			if ret != 0 {
				// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
				rt := sy.pTran.Deletebuff(id)
				if rt != 0 {
					sy.Logger.Errorf("delete data from standby fdfs failed,id:%+v", data.Data.ID)
					return -1
				}
			}
			// 更新二级索引中的id，换成备份集群的id
			indexcache.Item[i].Id = id
		} else {
			sy.Logger.Errorf("put data to standby fdfs failed,id: %+v", data.Data.ID)
			return -1
		}
	} else {
		sy.Logger.Errorf(" get data to master fdfs failed,id: %+v", data.Data.ID)
		return -1
	}

	return 1
}

func (sy *SyncMgr) cunsumeFdfsCh() {
	for {
		ctx, isClose := <-sy.pFdfsCh
		if !isClose {
			sy.Logger.Infof("channel closed!")
			return
		}

		i := ctx.Number
		indexcache := ctx.Cache
		data := ctx.Data
		length := ctx.Length

		ResIndexMap := ctx.ResIndex
		ResIndexMap[i] = sy.process(i, indexcache, data)

		isFinished := true
		for j := 0; j < length; j++ {
			if ResIndexMap[j] == 0 {
				isFinished = false
				break
			}
		}

		if !isFinished {
			continue
		}

		//获取所有成功的任务
		var delMap []int
		for j := 0; j < length; j++ {
			if ResIndexMap[j] != -1 {
				delMap[j] = j
			}
		}

		//比较是否有失败的任务，删除所有id对应于备份集群的buff
		delLen := len(delMap)
		if delLen != length {
			var wt sync.WaitGroup
			for j := 0; j < delLen; j++ {
				index := delMap[j]
				wt.Add(1)
				go func() {
					defer wt.Done()
				}()
			}
			
			wt.Wait()
		}

	}
	return
}

func (sy *SyncMgr) sendFileToBackupFdfs(indexcache *protocal.IndexCache, data *protocal.MsgMysqlBody) int {
	// 循环索引记录，分别对每一天记录进行从tair获取id，通过id从fdfs获取文件内容，然后发送到同步服务器
	length := len(indexcache.Item)

	var oldindex string
	filesize1 := fmt.Sprintf("%s\n", indexcache.FileSize)
	oldindex = oldindex + filesize1
	for i := 0; i < length; i++ {
		line := fmt.Sprintf("%s %s %s %s %s\n", indexcache.Item[i].Name, indexcache.Item[i].Id,
			indexcache.Item[i].Status, indexcache.Item[i].Size, indexcache.Item[i].Md5)
		oldindex = oldindex + line
	}
	//sy.Logger.Infof("old index:\n%+v", oldindex)

	resultIndex := make([]int, length)
	for i := 0; i < length; i++ {
		msg := protocal.Ctx{
			Number:   i,
			Length:   length,
			Cache:    indexcache,
			Data:     data,
			ResIndex: resultIndex,
			ResCh:    sy.pResCh,
		}
		sy.pFdfsCh <- msg
	}

	syncRes, isClose := <-sy.pResCh
	if !isClose {
		sy.Logger.Infof("channel closed!")
		return -1
	}

	sy.Logger.Infof("sync ID: %+v result: %+v", data.Data.ID, syncRes)

	//
	//
	//		// 根据二级索引中的id，从本集群获取对应的内容
	//		r, buf := sy.getFileFromFdfs(indexcache.Item[i].Id)
	//		if r == 0 {
	//			// 将二级索引中每一片内容上传到备份的fdfs中
	//			id := sy.pTran.Sendbuff(buf, data.Data.TaskID)
	//			if id != "" {
	//				// 将返回的id存储到备份集群的tair中
	//				//				ret := sy.putIndexFile(data, id)
	//				//				if ret != 0 {
	//				//					// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
	//				//					rt := sy.pTran.Deletebuff(id)
	//				//					if rt != 0 {
	//				//						sy.Logger.Errorf("delete data from standby fdfs failed. taskid:%+v, id:%+v",
	//				//							data.Data.TaskId, id)
	//				//						return -1
	//				//					}
	//				//				}
	//				// 更新二级索引中的id，换成备份集群的id
	//				indexcache.Item[i].Id = id
	//			} else {
	//				sy.Logger.Errorf("put data to standby fdfs failed. taskid: %+v",
	//					data.Data.TaskID)
	//				return -1
	//			}
	//		} else {
	//			sy.Logger.Errorf(" get data to master fdfs failed. taskId: %+v", data.Data.TaskID)
	//			return -1
	//		}
	//	}

	sy.Logger.Infof("send slice buff data to standby fdfs successful, taskid: %+v", data.Data.TaskID)

	//二级索引的所有文件已经转移完毕，请将二级索引文件上传到备份集群的fastdfs并且存储id到备份集群的tair
	var newindex string
	filesize := fmt.Sprintf("%s\n", indexcache.FileSize)
	newindex = newindex + filesize
	for i := 0; i < length; i++ {
		line := fmt.Sprintf("%s %s %s %s %s\n", indexcache.Item[i].Name, indexcache.Item[i].Id,
			indexcache.Item[i].Status, indexcache.Item[i].Size, indexcache.Item[i].Md5)
		newindex = newindex + line
	}
	//sy.Logger.Infof("new index:\n%+v", newindex)

	// 存储新的二级索引内容到备份集群的fdfs中
	buf := []byte(newindex)
	id := sy.pTran.Sendbuff(buf, data.Data.TaskID)
	if id == "" {
		// 二级索引内容存储失败，删除备份集群里该二级索引中每个id片对应的内容
		sy.Logger.Errorf("send index data to standby fdfs failed, taskid: %+v", data.Data.TaskID)
		for i := 0; i < length; i++ {
			// 存储到备份集群的tair失败，删除备份集群中id对于的buff
			rt := sy.pTran.Deletebuff(indexcache.Item[i].Id)
			if rt != 0 {
				sy.Logger.Errorf("delete data from standby fdfs failed, taskid: %+v, id: %+v",
					data.Data.TaskID, indexcache.Item[i].Id)
				return -1
			}
		}
		return -1
	}

	sy.Logger.Infof("send index data to standby fdfs successful, taskid: %+v",
		data.Data.TaskID)

	// 存储新的二级索引的id到备份集群的tair中
	ret := sy.putIndexFile(data, id)
	if ret != 0 {
		sy.Logger.Errorf("send index id to standby tair failed, taskid: %+v, id: %+v",
			data.Data.TaskID, id)
		// 二级索引id存储失败，删除备份集群里该二级索引中每个id片对应的内容
		for i := 0; i < length; i++ {
			// 存储到备份集群的tair失败，删除备份集群中id对于的buff
			rt := sy.pTran.Deletebuff(indexcache.Item[i].Id)
			if rt != 0 {
				sy.Logger.Errorf("delete data from standby fdfs failed, taskid: %+v, id: %+v",
					data.Data.TaskID, indexcache.Item[i].Id)
				return -1
			}
		}

		// 二级索引存储失败，删除备份集群里该二级索引的id对应的内容
		rt := sy.pTran.Deletebuff(id)
		if rt != 0 {
			sy.Logger.Errorf("delete data from standby fdfs failed, taskid: %+v, id :%+v",
				data.Data.TaskID, id)
		}
		return -1
	}

	sy.Logger.Infof("send index id to standby tair successful, taskid: %+v", data.Data.TaskID)

	// 向备份集群的mysql同步数据
	rt := sy.pSql.SendMysqlbuff(&data.Data, data.TableName)
	if rt != 0 {
		sy.Logger.Errorf("send db data to standby mysql failed, taskid: %+v", data.Data.TaskID)
		return -1
	}

	sy.Logger.Infof("send db data to standby mysql successful, taskid: %+v", data.Data.TaskID)
	sy.Logger.Infof("sy data successful, taskid: %+v", data.Data.TaskID)

	return 0
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

func (sy *SyncMgr) putIndexFile(data *protocal.MsgMysqlBody, id string) int {
	//	creat_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.CreateTime)
	//	creat_time_u := creat_time.Unix()
	//
	//	expiry_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.ExpiryTime)
	//	expiry_time_u := expiry_time.Unix()

	keys := protocal.SendTairPut{
		Prefix:     data.Data.Domain,
		Key:        data.Data.URI + ".index",
		Value:      id,
		CreateTime: data.Data.CreateTime,
		ExpireTime: data.Data.ExpiryTime,
	}

	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)
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

func (sy *SyncMgr) sendToEs(pData *protocal.DbInfo) int {
	tmp_time := strings.Split(pData.CreateTime, " ")

	if len(tmp_time) < 2 {
		sy.Logger.Infof("pData:%+v", pData)
		return -1
	}
	creat_time := fmt.Sprintf("%s%s%s%s", tmp_time[0], "T", tmp_time[1], "+08:00")

	var action int32
	if pData.SourceType == "DEL" {
		action = 1
	} else {
		action = 0
	}

	msg := &protocal.SendEsBody{
		TaskId:     pData.TaskID,
		Action:     action,
		Domain:     pData.Domain,
		FileName:   pData.FileName,
		Filesize:   pData.FileSize,
		Uri:        pData.URI,
		CreateTime: creat_time,
	}

	return sy.pEs.HandlerSendToEs(msg)
}

func (sy *SyncMgr) getFromEs(pData *protocal.DbInfo) int {
	msg := &protocal.GetEsInput{
		Domain:   pData.Domain,
		FileName: pData.FileName,
	}
	// 获取指定用户的文件
	return sy.pEs.HandlerGetFromEs(msg)
}

// 保存最后收到的task信息
func (sy *SyncMgr) SaveLastSuccessIdToFile(id int, tablename string) int {
	return sy.pBin.WriteTaskIdTofile(id, tablename)
}

func (sy *SyncMgr) InsertFailedTask(data *protocal.MsgMysqlBody) int {
	return sy.pSql.InsertFailedTask(&data.Data, data.TableName)
}

func (sy *SyncMgr) DeleteFailedTask(data *protocal.MsgMysqlBody) int {
	return sy.pSql.DeleteFailedTask(data.Data.TaskID, data.TableName)
}

func (sy *SyncMgr) NonUploadMachineSync(data *protocal.MsgMysqlBody, insertdb bool) int {
	result := 0
	// 获取索引
	ret, buff := sy.getIndexFileFromFdfs(&data.Data)
	if ret != 0 {
		sy.Logger.Errorf("getIndexFileFromFdfs failed, domain:%+v, prefix:+%v",
			data.Data.Domain, data.Data.URI)
		result = -1
	} else {
		// 解析索引
		var indexcache protocal.IndexCache
		err := indexmgr.ReadLine(buff, &indexcache)
		//sy.Logger.Infof("indexcache: %+v", indexcache)
		if err != nil {
			sy.Logger.Errorf("ReadLine err:%+v,taskid: %+v", err, data.Data.TaskID)
			result = -1
		}

		// 发送二级索引中每个id片对应的内容到备份集群
		if sy.sendFileToBackupFdfs(&indexcache, data) != 0 {
			sy.Logger.Errorf("sendFileToBackupFdfs failed,taskid: %+v", data.Data.TaskID)
			result = -1
		}
	}

	if result != 0 {
		// 记录上传失败的内容，有定时线程从数据库获取二级索引文件，重新上传
		if insertdb && sy.InsertFailedTask(data) != 0 {
			sy.Logger.Errorf("InsertFailedInfo failed tablename: %+v, taskid: %+v",
				data.TableName, data.Data.TaskID)
		}
	}

	return result
}

func (sy *SyncMgr) UploadMachineSync(data *protocal.MsgMysqlBody, insertdb bool) int {
	msg := &protocal.UploadInfo{
		TaskId:   data.Data.TaskID,
		Domain:   data.Data.Domain,
		FileName: data.Data.FileName,
		FileType: data.Data.FileType,
		Url:      data.Data.URI,
		CbUrl:    data.Data.CbURL,
		Behavior: "UP",
		Md5Type:  0,
		IsBackup: data.Data.IsBackup,
	}

	// 发送到上传机
	ret, _ := sy.SendUploadServer(msg)
	if ret == -1 {
		sy.Logger.Errorf("SendUploadServer failed")
		// 记录上传失败的内容，有定时线程反复上传该数据
		if insertdb && sy.InsertFailedTask(data) != 0 {
			sy.Logger.Errorf("InsertFailedInfo failed taskid:%+v", data.Data.TaskID)
		}
		return -1
	}

	// 任务已经存在，删除失败表中的记录
	if ret == 2 {
		if sy.DeleteFailedTask(data) != 0 {
			sy.Logger.Errorf("DeleteFailedTask failed taskid:%+v", data.Data.TaskID)
		}
	}

	return 0
}

func (sy *SyncMgr) SyncData(data *protocal.MsgMysqlBody, insertdb bool) int {
	if openSync == 0 {
		// 给上传机发送请求数据迁移
		return sy.UploadMachineSync(data, insertdb)
	} else {
		// 本服务负责数据迁移
		return sy.NonUploadMachineSync(data, insertdb)
	}
}

// read binlog data from pipe
func (sy *SyncMgr) readIncreaseInfo() {
	sy.Logger.Infof("start readIncreaseInfo.")
	for {
		data := sy.pBin.Read()
		if data == nil {
			break
		}
		// 迁移数据到备份集群
		sy.SyncData(data, true)
		// 发送结构文件到es，用于统计数据使用, 对于失败的任务不会在统计
		ret := sy.sendToEs(&data.Data)
		if ret != 0 {
			sy.Logger.Errorf("sendToEs failed taskid:%+v", data.Data.TaskID)
		}
	}

	sy.Logger.Infof("stop readIncreaseInfo.")
	return
}
