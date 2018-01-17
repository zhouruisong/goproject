package sync

import (
	"../binlogmgr"
	"../esmgr"
	"../indexmgr"
	"../mysqlmgr"
	"../protocal"
	"../transfer"
	//	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	//	"io/ioutil"
	//	"net/http"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var openSync = 0

type SyncMgr struct {
	pBin         *binlogmgr.BinLogMgr
	pCl          *transfer.TransferMgr
	pSql         *mysqlmgr.MysqlMgr
	pEs          *esmgr.EsMgr
	Logger       *log.Logger
	UploadServer string
	LastTaskId   string
}

func NewSyncMgr(server string, my *binlogmgr.BinLogMgr,
	cl *transfer.TransferMgr, sql *mysqlmgr.MysqlMgr, es *esmgr.EsMgr, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pBin:         my,
		pCl:          cl,
		pSql:         sql,
		pEs:          es,
		Logger:       lg,
		UploadServer: server,
	}

	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sync *SyncMgr) SetFlag(flag int) {
	openSync = flag
}

func (sync *SyncMgr) SendUploadServer(data *protocal.DbInfo) (int, error) {
	msg := protocal.UploadInfo{
		TaskId:    data.TaskId,
		Domain:    data.Domain,
		FileName:  data.FileName,
		FileType:  data.FileType,
		SourceUrl: data.SourceUrl,
		CbUrl:     data.CbUrl,
		Behavior:  "UP",
		Md5Type:   data.Md5Type,
		IsBackup:  1,
	}

	buff, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed err:%v, msg:%+v", err, msg)
		return -1, err
	}

	url := fmt.Sprintf("http://%v/index.php?Action=LiveMaintain.FileTaskAddNew", sync.UploadServer)
	ip := strings.Split(sync.UploadServer, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	sync.Logger.Infof("url: %+v", url)
	sync.Logger.Infof("hosturl: %+v, buff:%+v", hosturl, string(buff))
	return 0, nil
	//	body := bytes.NewBuffer([]byte(buff))
	//	res, err := http.Post(url, hosturl, body)
	//	if err != nil {
	//		sync.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
	//		return -1, err
	//	}
	//
	//	defer res.Body.Close()
	//
	//	result, err := ioutil.ReadAll(res.Body)
	//	if err != nil {
	//		sync.Logger.Errorf("ioutil readall failed, err:%v, buff:%+v", err, string(buff))
	//		return -1, err
	//	}
	//
	//	var ret protocal.RetUploadMeg
	//	err = json.Unmarshal(result, &ret)
	//	if err != nil {
	//		sync.Logger.Errorf("Unmarshal return body error, err:%v, buff:%+v", err, string(buff))
	//		return -1, err
	//	}
	//
	//	sync.Logger.Infof("ret: %+v", ret)
	//
	//	if ret.Code == 0 {
	//		return 0, nil
	//	}
	//
	//	return -1, nil
}

func (sync *SyncMgr) sendFileToBackupFdfs(pMap *protocal.IndexMap, data *protocal.DbEventInfo) int {
	// 循环索引记录，分别对每一天记录进行从tair获取id，通过id从fdfs获取文件内容，然后发送到同步服务器
	for _, item := range pMap.Item {
		// get data from master fdfs according id
		r, buf := sync.getFileFromFdfs(item.Id)
		if r == 0 {
			// put data buff to standby fdfs
			id := sync.pCl.Sendbuff(buf, data.DbData.TaskId)
			sync.Logger.Infof("id: %+v", id)
			if id != "" {
				// put id to standby tair
				ret := sync.putIndexFile(data, id)
				sync.Logger.Infof("ret: %+v", ret)
				if ret != 0 {
					// delete data in standby fdfs use id if putIndexFile failed
					rt := sync.pCl.Deletebuff(id)
					sync.Logger.Infof("rt: %+v", rt)
					if rt != 0 {
						sync.Logger.Errorf("delete data from standby fdfs failed. TaskId:%+v, id:%+v",
							data.DbData.TaskId, id)
						return -1
					}
				}
				sync.Logger.Infof("id:%+v", id)

				// update index use new id
				item.Id = id
			} else {
				sync.Logger.Errorf("put data to standby fdfs failed. TaskId: %+v",
					data.DbData.TaskId)
				//return -1
			}
		} else {
			sync.Logger.Errorf(" get data to master fdfs failed. TaskId: %+v", data.DbData.TaskId)
			return -1
		}
	}

	sync.Logger.Infof("send slice buff data to standby fdfs successful, taskId: %+v", data.DbData.TaskId)

	//二级索引的所有文件已经转移完毕，请将二级索引文件上传到fastdfs并且存储id到tair
	var total string
	for _, item := range pMap.Item {
		line := fmt.Sprintf("%s %s %s %s %s\n", item.Name, item.Id, item.Status, item.Size, item.Md5)
		total = total + line
	}

	sync.Logger.Infof("index total:\n%+v", total)

	buf := []byte(total)
	// put new index data to standby fdfs
	id := sync.pCl.Sendbuff(buf, data.DbData.TaskId)
	if id == "" {
		sync.Logger.Errorf("put index data to standby fdfs failed. TaskId: %+v", data.DbData.TaskId)
		return -1
	}

	sync.Logger.Infof("put index data to standby fdfs successful, taskId: %+v", data.DbData.TaskId)

	// put index id to standby tair
	ret := sync.putIndexFile(data, id)
	if ret != 0 {
		// delete data of standby fdfs use id
		rt := sync.pCl.Deletebuff(id)
		if rt != 0 {
			sync.Logger.Errorf("delete data from standby fdfs failed. TaskId:%+v, id:%+v",
				data.DbData.TaskId, id)
		}
		sync.Logger.Errorf("put index id to standby tair failed. TaskId: %+v", data.DbData.TaskId)
		return -1
	}

	sync.Logger.Infof("put index id to standby tair successful, taskId: %+v", data.DbData.TaskId)

	// send db data to standby mysql
	rt := sync.pSql.SendMysqlbuff(&data.DbData, data.TableName)
	if rt != 0 {
		sync.Logger.Errorf("send db data to standby mysql failed. TaskId: %+v", data.DbData.TaskId)
		return -1
	}

	sync.Logger.Infof("sync db data successful, taskId: %+v", data.DbData.TaskId)

	return 0
}

func (sync *SyncMgr) getFileFromFdfs(id string) (int, []byte) {
	var ret []byte
	// get file from fdfs
	rt, fileBuff := sync.pCl.PFdfs.HandlerDownloadFile(id)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ret
	}

	//sync.Logger.Infof("fileBuff: %+v", string(fileBuff))

	//	filename := pData.TaskId + pData.FileName
	//	file, _ := os.Create(filename)
	//	defer file.Close()
	//	file.Write(fileBuff)

	return 0, fileBuff
}

func (sync *SyncMgr) putIndexFile(data *protocal.DbEventInfo, id string) int {
	//	creat_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.CreateTime)
	//	creat_time_u := creat_time.Unix()
	//
	//	expiry_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.ExpiryTime)
	//	expiry_time_u := expiry_time.Unix()

	keys := protocal.SendTairPut{
		Prefix:     data.DbData.Domain,
		Key:        data.DbData.FfUri,
		Value:      id,
		CreateTime: data.DbData.CreateTime,
		ExpireTime: data.DbData.ExpiryTime,
	}

	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)

	buf, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return -1
	}

	var ret protocal.RetTairPut
	ret.Errno, ret.Errmsg = sync.pCl.PTair.HandlerSendtoTairPut(buf)

	return ret.Errno
}

func (sync *SyncMgr) getIndexFile(prefix string, key string) *protocal.RetTairGet {
	keys := protocal.SendTairGet{
		Prefix: prefix,
		Key:    "/" + prefix + key,
	}

	var msg protocal.SednTairGetBody
	msg.Keys = append(msg.Keys, keys)

	buf, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return nil
	}

	var ret protocal.RetTairGet
	ret.Errno, ret.Keys = sync.pCl.PTair.HandlerSendtoTairGet(buf)

	return &ret
}

func (sync *SyncMgr) getIndexFileFromFdfs(pData *protocal.DbInfo) (int, string) {
	// first get index id from tair
	ret := sync.getIndexFile(pData.Domain, pData.FfUri)
	if ret.Errno != 0 {
		sync.Logger.Errorf("ret: %+v", ret)
		return -1, ""
	}

	sync.Logger.Infof("ret: %+v", ret)

	// get index file from fdfs
	rt, fileBuff := sync.pCl.PFdfs.HandlerDownloadFile(ret.Keys[0].Value)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ""
	}

	sync.Logger.Infof("fileBuff: %+v", string(fileBuff))

	filename := pData.TaskId + pData.FileName
	file, _ := os.Create(filename)
	defer file.Close()
	file.Write(fileBuff)

	return 0, filename
}

func (sync *SyncMgr) sendToEs(pData *protocal.DbInfo) int {
	tmp_time := strings.Split(pData.CreateTime, " ")
	creat_time := fmt.Sprintf("%s%s%s%s", tmp_time[0], "T", tmp_time[1], "+08:00")

	var action int32
	if pData.Action == "UP" {
		action = 0
	} else {
		action = 1
	}

	msg := &protocal.SendEsBody{
		TaskId:     pData.TaskId,
		Action:     action,
		Domain:     pData.Domain,
		FileName:   pData.FileName,
		Filesize:   pData.FileSize,
		FfUri:      pData.FfUri,
		CreateTime: creat_time,
	}

	return sync.pEs.HandlerSendToEs(msg)
}

func (sync *SyncMgr) getFromEs(pData *protocal.DbInfo) int {
	msg := &protocal.GetEsInput{
		Domain:   pData.Domain,
		FileName: pData.FileName,
	}
	// 获取指定用户的文件
	return sync.pEs.HandlerGetFromEs(msg)
}

// save SendUploadServer faild info to file
func (sync *SyncMgr) SaveDbDataToFile(data *protocal.DbInfo) {
	content := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%d,%d", data.TaskId, data.Domain,
		data.FileName, data.FileType, data.SourceUrl, data.CbUrl, "UP", data.Md5Type, 1)
	sync.pBin.Tracefile(content)
}

// save SendUploadServer faild info to file
func (sync *SyncMgr) SaveLastSuccessIdToFile(taskid, tablename string) {
	sync.pBin.WriteTaskIdTofile(taskid, tablename)
}

// read binlog data from pipe
func (sync *SyncMgr) readIncreaseInfo() {
	sync.Logger.Infof("start readIncreaseInfo.")
	for {
		data := sync.pBin.Read()
		sync.Logger.Infof("data: %+v", data)

		// 给上传机发送请求，从源机器获取文件
		if openSync == 0 {
			_, err := sync.SendUploadServer(&data.DbData)
			if err != nil {
				sync.Logger.Errorf("SendUploadServer failed err:%+v", err)
				// 记录上传失败的内容，有定时线程反复上传该数据
				sync.SaveDbDataToFile(&data.DbData)
				continue
			} else {
				// 更新最后一次成功上传的tastkId，重启服务会从这个id开始到最后一条tastid继续上传
				sync.LastTaskId = data.DbData.TaskId
				sync.SaveLastSuccessIdToFile(sync.LastTaskId, data.TableName)
			}
		} else {
			// 获取索引
			rt, filename := sync.getIndexFileFromFdfs(&data.DbData)
			if rt != 0 {
				sync.Logger.Errorf("getIndexFileFromFdfs failed, domain:%+v, prefix:+%v",
					data.DbData.Domain, data.DbData.FfUri)
				continue
			} else {
				// 解析索引
				var indexmap protocal.IndexMap
				err := indexmgr.ReadLine(filename, &indexmap)
				sync.Logger.Infof("indexmap: %+v", indexmap)
				if err != nil {
					sync.Logger.Errorf("ReadLine err:%+v, filename:%+v", err, filename)
					continue
				}

				// 索引中的每个文件分别获取后，发送到同步服务器
				// get file buff of index and send it to backup fdfs
				rt = sync.sendFileToBackupFdfs(&indexmap, data)
				if rt != 0 {
					sync.Logger.Errorf("sendFileToBackupFdfs failed rt:%+v, DbData:%+v", rt, data.DbData)
					continue
				}
			}
		}

		// 发送结构文件到es，用于统计数据使用
		ret := sync.sendToEs(&data.DbData)
		if ret != 0 {
			sync.Logger.Errorf("sendToEs failed ret:%+v, DbData:%+v", ret, data.DbData)
		}
	}

	return
}

func (sync *SyncMgr) IncreaseSync() {
	sync.Logger.Infof("start IncreaseSync.")

	go sync.pBin.RunMoniterMysql()

//	for i := 0; i < 1; i++ {
//		go sync.readIncreaseInfo()
//	}

	//	go sync.InsertToDb()
	//	time.Sleep(1 * time.Second)
	//	go sync.InsertToDb()
	//	time.Sleep(1 * time.Second)
	//	go sync.InsertToDb()
	return
}

func (sync *SyncMgr) InsertToDb() {
	// test insert
	time.Sleep(10 * time.Second)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//10以内的随机数
	num := int(r.Int31n(1000))

	sync.Logger.Infof("start insert.")
	for i := 0; i < num; i++ {
		var filesize int32

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		//10以内的随机数
		filesize = int32(r.Int31n(100))

		taskid := "zhouruisong" + strconv.Itoa(i)
		filename := taskid + ".mp4"
		Md5 := fmt.Sprintf("%s%s", "0fb2eae9de4c1d4", taskid)
		url := fmt.Sprintf("%s/%s", "http://twin14602.sandai.net/tools/coral", filename)

		time.Sleep(1 * time.Second)

		a := time.Now()
		creat_time := fmt.Sprintf("%s", a.Format("2006-01-02 15:04:05"))

		info := protocal.DbInfo{
			TaskId:     taskid,
			FileName:   filename,
			FileType:   "mp4",
			FileSize:   filesize,
			Domain:     "www.wasu.cn",
			Status:     200,
			Action:     "UP",
			Md5Type:    1,
			CreateTime: creat_time,
			DnameMd5:   Md5,
			SourceUrl:  url,
			FileMd5:    Md5,
			CbUrl:      url,
			FfUri:      url,
			Type:       0,
		}
		sync.pSql.InsertMultiStreamInfosTest(info, "t_livefcup")
	}

	sync.Logger.Infof("insert end.")

}
