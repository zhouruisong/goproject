package sync

import (
	// "io"
	// "os"
	"../domainmgr"
	"../fdfs_client"
	"../tair"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	mutex  sync.Mutex
	id     uint32
	number int
)

type RetCheckDataExist struct {
	Errno     int    `json:"code"`
	Errmsg    string `json:"message"`
	Tablename string `json:"tablename"`
	Taskid    string `json:"taskid"`
	Count     int    `json:"count"`
}

type RetSendStream struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

type RetSendBuff struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

type MsgStreamBody struct {
	TableName string                 `json:"tablename"`
	Data      []domainmgr.StreamInfo `json:"data"`
}

// filetype=1 m3u8, filetype=0 ts
type MsgSendBuff struct {
	FileType    int    `json:"filetype"`
	Domain      string `json:"domain"`
	FileName    string `json:"filename"`
	App         string `json:"app"`
	PublishTime uint64 `json:"publishtime"`
	Content     []byte `json:"content"`
}

type SyncMgr struct {
	pMy     *domainmgr.BinLogMgr
	pDevmgr *domainmgr.StreamMgr
	pTair   *tair.TairClient
	pFdfs   *fdfs_client.FdfsClient
	Logger  *log.Logger
}

func (sync *SyncMgr) write(newid uint32) {
	mutex.Lock()
	defer mutex.Unlock()
	// 省略若干条语句
	id = newid
}

func (sync *SyncMgr) read() uint32 {
	mutex.Lock()
	defer mutex.Unlock()
	// 省略若干条语句
	return id
}

func NewSyncMgr(dm *domainmgr.StreamMgr, my *domainmgr.BinLogMgr, tr *tair.TairClient, fdfs *fdfs_client.FdfsClient, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pMy:     my,
		pDevmgr: dm,
		pTair:   tr,
		pFdfs:   fdfs,
		Logger:  lg,
	}
	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sync *SyncMgr) sendStreamInfo(arrayInfo []domainmgr.StreamInfo, tablename string) int {
	msg := &MsgStreamBody{
		TableName: tablename,
		Data:      arrayInfo,
	}

	ret, _ := sync.SendDbData(msg)
	if ret != 0 {
		return 1
	}

	return 0
}

func (sync *SyncMgr) SendTs(domain, head, line string) {
	tsKey := head + "/" + line
	sync.Logger.Infof("tsKey: %+v", tsKey)

	info := &domainmgr.StreamInfo{
		FileName: tsKey,
		Domain:   domain,
	}

	sync.Logger.Infof("info: %+v", info)
	ids, err := sync.pTair.SendtoTairGet(info)
	if err != nil || len(ids) == 0 {
		return
	}

	for _, id := range ids {
		buff, err := sync.DownloadFileToBuffer(id)
		if err != nil {
			sync.Logger.Errorf("DownloadFileToBuffer fail: %+v", id)
			return
		}

		msg := &MsgSendBuff{
			FileType: 0,
			FileName: tsKey,
			Content:  buff,
		}

		// send m3u8 file to backup cluster
		_, err = sync.SendBuff(msg)
		if err != nil {
			sync.Logger.Errorf("SendBuff fail, need to retry, info: %+v", msg)
			return
		}
	}

	return
}

func (sync *SyncMgr) readLine(info *domainmgr.StreamInfo, buf []byte, handler func(string, string, string)) error {
	s := strings.Split(info.FileName, "/")
	sync.Logger.Infof("s: %+v", s)
	tskeyhead := s[0] + "/" + s[1]
	sync.Logger.Infof("tskeyhead: %+v", tskeyhead)

	// var b bytes.Buffer
	b := bytes.NewBuffer(buf)
	// b.Write(buf)

	for {
		line, err := b.ReadString('\n')
		if err == nil {
			break
		}

		line = strings.TrimSpace(line)
		// skip if line contain "#"
		if strings.Contains(line, "#") {
			continue
		}

		handler(info.Domain, tskeyhead, line)
	}
	return nil
}

func (sync *SyncMgr) readIncreaseInfo() {
	sync.Logger.Infof("start readIncreaseInfo.")
	for {
		data := sync.pMy.Read()
		info := data.Info
		ret, err := sync.SendCheckData(&info, data.Tablename)
		if err != nil {
			sync.Logger.Errorf("SendCheckData ret:%v", ret)
		} else {
			if ret == 0 {
				var sendStream []domainmgr.StreamInfo
				sendStream = append(sendStream, info)
				rt := sync.sendStreamInfo(sendStream, data.Tablename)
				if rt != 0 {
					sync.Logger.Errorf("sendStreamInfo fail, need to retry, info: %+v", info)
					continue
				}

				// get M3U8 file address from tair into buff
				// ids, err := sync.pTair.SendtoTairGet(&info)
				// if err != nil {
				// 	sync.Logger.Errorf("SendtoTairGet fail, need to retry, info: %+v", info)
				// 	continue
				// }

				// for _, id := range ids {
				// 	buff, err := sync.DownloadFileToBuffer(id)
				// 	if err != nil {
				// 		sync.Logger.Errorf("DownloadFileToBuffer fail: %+v", id)
				// 		continue
				// 	}

				// 	msg := &MsgSendBuff{
				// 		FileType: 1,
				// 		Domain:   info.Domain,
				// 		FileName: info.FileName,
				// 		App: info.App,
				// 		PublishTime: info.PublishTime,
				// 		Content:  buff,
				// 	}

				// 	// send m3u8 file to backup cluster
				// 	ret, err = sync.SendBuff(msg)
				// 	if err != nil {
				// 		sync.Logger.Errorf("SendBuff fail, need to retry, info: %+v", msg)
				// 		continue
				// 	}

				// 	// send ts file in m3u8 to backup cluster
				// 	sync.readLine(&info, buff, sync.SendTs)
				// 	sync.Logger.Errorf("sendM3u8 fail: %+v", id)
				// }
			}

		}
	}

	return
}

func (sync *SyncMgr) IncreaseSync() {
	sync.Logger.Infof("start IncreaseSync.")

	go sync.pMy.RunMoniterMysql()

	for i := 0; i < 2000; i++ {
		go sync.readIncreaseInfo()
	}

	// test insert
	time.Sleep(10 * time.Second)
	for i := 0; i < 1; i++ {
		sync.pDevmgr.InsertStreamInfos(i)
	}

	return
}

func (sync *SyncMgr) TotalSync() {
	sync.Logger.Infof("start TotalSync.")
	tablenames, err := sync.pDevmgr.SelectTableName()
	if err != nil {
		return
	}

	for _, name := range tablenames {
		go sync.StartTableSync(name)
	}
}

func (sync *SyncMgr) DownloadFileToBuffer(id string) ([]byte, error) {
	downloadResponse, err := sync.pFdfs.DownloadToBuffer(id, 0, 0)
	if err != nil {
		sync.Logger.Errorf("DownloadToBuffer fail: %+v", id)
		return nil, err
	}
	sync.Logger.Infof("downloadResponse: %+v", downloadResponse)

	var buf []byte
	if value, ok := downloadResponse.Content.([]byte); ok {
		return value, nil
	}

	return buf, nil
}

func (sync *SyncMgr) StartTableSync(tablename string) {
	start := time.Now()
	beginIndex := 0
	for {
		sync.Logger.Infof("StartTableSync beginIndex: %v, start", beginIndex)
		info, ret := sync.pDevmgr.LoadStreamInfos(beginIndex, tablename)
		if ret != 0 {
			sync.Logger.Errorf("LoadStreamInfos failed, beginIndex: %v", beginIndex)
			break
		}

		rt := sync.sendStreamInfo(info, tablename)
		if rt != 0 {
			sync.Logger.Errorf("sendStreamInfo fail, beginIndex: %v", beginIndex)
			continue
		}

		sync.Logger.Infof("StartTableSync beginIndex: %v, finished", beginIndex)
		beginIndex++
	}
	end := time.Now()
	sync.Logger.Infof("tablename %v StartTableSync ok, total time: [%v]s",
		tablename, end.Sub(start).Seconds())
}

func (sync *SyncMgr) SendCheckData(info *domainmgr.StreamInfo, tablename string) (int, error) {
	var url string
	url = fmt.Sprintf("http://%v:3000/checkdata", sync.pDevmgr.MysqlBackupList)
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", sync.pDevmgr.MysqlBackupList)

	msg := RetCheckDataExist{
		Errno:     -1,
		Errmsg:    "",
		Tablename: tablename,
		Taskid:    info.TaskId,
		Count:     -1,
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, buf: %+v", err, string(buf))
		return 1, err
	}

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		sync.Logger.Errorf("url:%v, buf: %+v", url, string(buf))
		return 1, err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ioutil readall failed.err:%v", err)
		return 1, err
	}

	var ret RetCheckDataExist
	err = json.Unmarshal(result, &ret)
	if err != nil {
		sync.Logger.Errorf("cannot decode req body Error, result:%v", err)
		if e, ok := err.(*json.SyntaxError); ok {
			sync.Logger.Errorf("syntax error at byte offset %d", e.Offset)
		}

		sync.Logger.Errorf("sakura response: %q", result)
		return 1, err
	}

	if ret.Errno != 0 {
		sync.Logger.Infof("SendCheckData return Errno !=0 info:%+v", info)
		// record this id for retry send
		return 1, nil
	}

	sync.Logger.Infof("SendCheckData return ok ret: %+v, hosturl: %v", ret, hosturl)
	return 0, nil
}

func (sync *SyncMgr) RecviveCheckData(res http.ResponseWriter, req *http.Request) {
	var ret int
	var q RetCheckDataExist
	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
		q.Errno = -1
		q.Errmsg = "ioutil.ReadAll failed"
		q.Count = -1
		goto END
	}

	err = json.Unmarshal(buf, &q)
	if err != nil {
		sync.Logger.Errorf("Error: cannot decode req body %v", err)
		q.Errno = -1
		q.Errmsg = "json.Unmarshal failed"
		q.Count = -1
		goto END
	}

	sync.Logger.Infof("RecviveCheckData recive buf: %+v", string(buf))
	ret = sync.pDevmgr.SelectDataExist(q.Taskid, q.Tablename)
	q.Count = ret
	q.Errno = 0
	q.Errmsg = "ok"

END:
	b, err := json.Marshal(&q)
	if err != nil {
	}

	sync.Logger.Infof("RecviveCheckData return q: %+v, buf: %+v", q, string(buf))
	res.Write(b) // HTTP 200
}

func (sync *SyncMgr) SendBuff(pMsg *MsgSendBuff) (int, error) {
	var url string
	url = fmt.Sprintf("http://%v:3000/receivebuff", sync.pDevmgr.MysqlBackupList)
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", sync.pDevmgr.MysqlBackupList)

	buf, err := json.Marshal(pMsg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, buf: %+v", err, string(buf))
		return 1, err
	}

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		sync.Logger.Errorf("url:%v, buf: %+v", url, string(buf))
		return 1, err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ioutil readall failed.err:%v", err)
		return 1, err
	}

	var ret RetSendStream
	err = json.Unmarshal(result, &ret)
	if err != nil {
		sync.Logger.Errorf("cannot decode req body Error, result:%v", err)
		if e, ok := err.(*json.SyntaxError); ok {
			sync.Logger.Errorf("syntax error at byte offset %d", e.Offset)
		}

		sync.Logger.Errorf("sakura response: %q", result)
		return 1, err
	}

	if ret.Errno != 0 {
		sync.Logger.Errorf("result return failed! code:%v, msg: %v, buf:%+v", ret.Errno, ret.Errmsg, string(buf))
		// record this id for retry send
		return 1, nil
	}

	sync.Logger.Infof("SendBuff return ok! ret: %+v", ret)
	return 0, nil
}

func (sync *SyncMgr) SendDbData(pMsg *MsgStreamBody) (int, error) {
	var url string
	url = fmt.Sprintf("http://%v:3000/mysqlsync", sync.pDevmgr.MysqlBackupList)
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", sync.pDevmgr.MysqlBackupList)

	buf, err := json.Marshal(pMsg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, buf: %+v", err, string(buf))
		return 1, err
	}

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		sync.Logger.Errorf("url:%v, buf: %+v", url, string(buf))
		return 1, err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ioutil readall failed.err:%v", err)
		return 1, err
	}

	var ret RetSendStream
	err = json.Unmarshal(result, &ret)
	if err != nil {
		sync.Logger.Errorf("cannot decode req body Error, result:%v", err)
		if e, ok := err.(*json.SyntaxError); ok {
			sync.Logger.Errorf("syntax error at byte offset %d", e.Offset)
		}

		sync.Logger.Errorf("sakura response: %q", result)
		return 1, err
	}

	if ret.Errno != 0 {
		sync.Logger.Errorf("result return failed! code:%v, msg: %v, buf:%+v", ret.Errno, ret.Errmsg, string(buf))
		// record this id for retry send
		return 1, nil
	}

	sync.Logger.Infof("SendDbData return ok! ret: %+v", ret)
	return 0, nil
}

// 接收DB同步过来的内容，插入对应的live_master表中
func (sync *SyncMgr) ReceiveDbData(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ReadAll failed. %v", err)
	}

	rt := sync.handlerdbinfo(buf)

	r := RetSendStream{
		Errno:  rt,
		Errmsg: "ok",
	}

	b, err := json.Marshal(&r)
	if err != nil {
		sync.Logger.Errorf("Marshal failed. %v", err)
	}

	res.Write(b) // HTTP 200
}

// 处理函数
func (sync *SyncMgr) handlerdbinfo(buf []byte) int {
	if len(buf) == 0 {
		sync.Logger.Errorf("buf len = 0")
		return -1
	}

	var q MsgStreamBody
	err := json.Unmarshal(buf, &q)
	if err != nil {
		sync.Logger.Errorf("Error: cannot decode req body %v", err)
		return -1
	}

	number = number + len(q.Data)
	sync.Logger.Infof("handlerdbinfo len: %+v, number: %+v", len(q.Data), number)

	ret := sync.pDevmgr.InsertMultiStreamInfos(q.Data, q.TableName)
	if ret != 0 {
		sync.Logger.Errorf("InsertMultiStreamInfos failed")
		return -1
	}

	return 0
}

// 接收发送的文件消息，存入fastdfs，id写入tair
func (sync *SyncMgr) ReceiveBuff(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ReadAll failed. %v", err)
	}

	rt := sync.handlerbuff(buf)
	r := RetSendStream{
		Errno:  rt,
		Errmsg: "ok",
	}

	b, err := json.Marshal(&r)
	if err != nil {
		sync.Logger.Errorf("Marshal failed. %v", err)
	}

	res.Write(b) // HTTP 200
}

// 处理函数
func (sync *SyncMgr) handlerbuff(buf []byte) int {
	if len(buf) == 0 {
		sync.Logger.Errorf("buf len = 0")
		return -1
	}

	var q MsgSendBuff
	err := json.Unmarshal(buf, &q)
	if err != nil {
		sync.Logger.Errorf("Error: cannot decode req body %v", err)
		return -1
	}

	sync.Logger.Infof("handlerdbinfo q: %+v", q)

	uploadres, err := sync.pFdfs.UploadAppenderByBuffer(q.Content, q.FileName)
	if err != nil {
		sync.Logger.Errorf("UploadAppenderByBuffer failed err: %v", err)
		return -1
	}

	sync.Logger.Infof("UploadAppenderByBuffer ok uploadres: %+v", uploadres)

	tairput := &domainmgr.StreamInfo{
		Domain:      q.Domain,
		FileName:    q.FileName,
		App:         q.App,
		PublishTime: q.PublishTime,
	}

	err = sync.pTair.SendtoTairPut(tairput, uploadres.RemoteFileId)
	if err != nil {
		sync.Logger.Errorf("SendtoTairPut failed")
		return -1
	}

	sync.Logger.Infof("SendtoTairPut ok")
	return 0
}
