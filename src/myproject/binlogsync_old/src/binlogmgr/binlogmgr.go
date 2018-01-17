package binlogmgr

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	//	"reflect"
	"database/sql"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

var (
	host             = "localhost"
	port             uint16
	username         = "root"
	password         = "123456"
	server_Id        uint32
	eachNum          = 100
	dbconns          = 200
	dbidleconns      = 100
	g_sync_part_time = false

	lastidfile string
	db         *sql.DB

	g_tastId_Fd *os.File = nil
	g_lastId    *os.File = nil
	g_TastIdMap          = make(map[string]string)
)

type TimeInfo struct {
	hour   int
	minute int
	second int
}

type BinLogMgr struct {
	Logger     *log.Logger
	EventChan  chan *protocal.DbEventInfo
	tablename  string
	start_time TimeInfo
	end_time   TimeInfo
}

func NewBinLogMgr(host_ string, port_ uint16, username_ string,
	password_ string, serverId_ uint32, each int, dbconn int,
	dbidle int, binlogfile string, lastidfile_ string,
	starttime string, endtime string, lg *log.Logger) *BinLogMgr {

	my := &BinLogMgr{
		Logger:    lg,
		EventChan: make(chan *protocal.DbEventInfo, 10000),
		tablename: "",
		start_time: TimeInfo{
			hour:   0,
			minute: 0,
			second: 0,
		},
		end_time: TimeInfo{
			hour:   23,
			minute: 59,
			second: 0,
		},
	}

	host = host_
	port = port_
	username = username_
	password = password_
	server_Id = serverId_
	eachNum = each
	dbconns = dbconn
	dbidleconns = dbidle
	lastidfile = lastidfile_

	rt := GetSyncTime(my, starttime, endtime)
	if rt != true {
		my.Logger.Errorf("GetSyncTime failed")
		return nil
	}

	g_tastId_Fd = my.fileCreate(binlogfile)
	if g_tastId_Fd == nil {
		my.Logger.Errorf("fileCreate:%v failed", binlogfile)
		return nil
	}

	g_lastId = my.fileCreate(lastidfile)
	if g_lastId == nil {
		my.Logger.Errorf("fileCreate:%v failed", lastidfile)
		return nil
	}

	my.Logger.Infof("sync starttime:%+v, sync endtime:%+v", my.start_time, my.end_time)
	my.Logger.Infof("g_sync_part_time:%+v", g_sync_part_time)
	my.Logger.Infof("NewBinLogMgr ok")
	return my
}

func (mgr *BinLogMgr) fileCreate(path string) *os.File {
	pFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		mgr.Logger.Errorf("Open:%v failed", path)
		defer pFile.Close()
		return nil
	}

	return pFile
}

func (mgr *BinLogMgr) pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//taskid内容写到文件中
func (mgr *BinLogMgr) Tracefile(str_content string) {
	fd_content := strings.Join([]string{str_content, "\n"}, "")
	buf := []byte(fd_content)
	g_tastId_Fd.Write(buf)
	g_tastId_Fd.Sync()
}

//taskid内容写到文件中
func (mgr *BinLogMgr) WriteTaskIdTofile(taskid, tablename string) {
	g_TastIdMap[tablename] = taskid

	for k, v := range g_TastIdMap {
		mgr.Logger.Infof("k:%+v, v:%+v", k, v)
	}

	// 清除文件内容
	err := os.Truncate(lastidfile, 0)
	if err != nil {
		mgr.Logger.Errorf("Truncate failed, err:%+v", err)
		return
	}

	// 遍历map
	for k, v := range g_TastIdMap {
		newContent := fmt.Sprintf("%s,%s", k, v)
		fd_content := strings.Join([]string{newContent, "\n"}, "")
		//		mgr.Logger.Infof("fd_content:%+v", fd_content)
		buf := []byte(fd_content)
		n, err := g_lastId.Write(buf)
		mgr.Logger.Infof("n:%+v", n)
		mgr.Logger.Infof("err:%+v", err)
		g_lastId.Sync()
	}
}

func (mgr *BinLogMgr) Read() *protocal.DbEventInfo {
	info := <-mgr.EventChan
	return info
}

func (mgr *BinLogMgr) Write(info *protocal.DbEventInfo) int {
	select {
	case mgr.EventChan <- info:
		return 0
	case <-time.After(time.Second * 2):
		mgr.Logger.Infof("write to channel timeout: %+v", info)
		return -1
	}
	return 0
}

func (mgr *BinLogMgr) StartPartTimeSync() {
	if g_sync_part_time && CheckStartEndTime(mgr) {
	}
}

func (mgr *BinLogMgr) RunMoniterMysql() {
	mgr.Logger.Infof("start run RunMoniterMysql")
	cfgsql := replication.BinlogSyncerConfig{
		ServerID: server_Id,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
	}

	syncer := replication.NewBinlogSyncer(&cfgsql)

	url := fmt.Sprintf("%s:%d", host, port)
	c, err := client.Connect(url, username, password, "live_master")
	if err != nil {
		mgr.Logger.Errorf("Connect db failed: %+v", url)
		panic(err.Error())
	}

	r, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		mgr.Logger.Errorf("Execute SHOW MASTER STATUS failed")
		panic(err.Error())
	}

	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	mgr.Logger.Infof("binFile:%+v, binPos:%+v", binFile, binPos)

	// Start sync with sepcified binlog file and position
	streamer, errnew := syncer.StartSync(mysql.Position{binFile, (uint32)(binPos)})
	if errnew != nil {
		mgr.Logger.Errorf("StartSync fail, err: %+v", errnew)
		panic(errnew.Error())
	}

	if streamer == nil {
		mgr.Logger.Errorf("streamer == nil")
		panic("streamer == nil")
	}

	for {
		ev, _ := streamer.GetEvent(context.Background())
		var r replication.Event
		r = ev.Event

		//		fmt.Printf("type: %+v", reflect.TypeOf(r))
		//		ev.Dump(os.Stdout)

		if evname, okname := r.(*replication.TableMapEvent); okname {
			mgr.tablename = fmt.Sprintf("%s", evname.Table)
		}

		if ev, ok := r.(*replication.RowsEvent); ok {
			mgr.GetDump(ev, mgr.tablename)
		}
	}
}

func (mgr *BinLogMgr) GetDump(ev *replication.RowsEvent, tablename string) {
	var data protocal.DbEventInfo
	for _, rows := range ev.Rows {
		for k, d := range rows {
			if _, ok := d.([]byte); ok {
				//				mgr.Logger.Infof("type: %+v", reflect.TypeOf(d))
			} else {
				GetValue(&data.DbData, k, d)
			}
		}
	}

	mgr.Logger.Infof("data: %+v\n", data)

	//data.Status == 200 表示是源数据处理完毕，需要同步， 为1表示是备份数据，不需要同步
	if data.DbData.IsBackup == 0 && data.DbData.Status == 200 {
		data.TableName = tablename
		// 给同步服务发消息
		if mgr.SendToClusterSync(&data) != 0 {
			mgr.Logger.Errorf("SendToClusterSync failed data:%+v", data)
		}
	}
}

func (mgr *BinLogMgr) SendToClusterSync(msg *protocal.DbEventInfo) int {
	buf, err := json.Marshal(msg)
	if err != nil {
		mgr.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return -1
	}

	url := fmt.Sprintf("http://%v/uploadput", "192.168.110.30:3000")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", "192.168.110.30")

	mgr.Logger.Infof("data: %+v\n", url)
	mgr.Logger.Infof("hosturl: %+v\n", hosturl)

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		mgr.Logger.Errorf("http post return failed. err: %v , msg: %+v", err, msg)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		mgr.Logger.Errorf("ioutil readall failed. err:%v", err)
		return -1
	}

	var ret protocal.MsgMysqlRet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		mgr.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return -1
	}

	mgr.Logger.Infof("ret: %+v", ret)

	if ret.Errno == 0 {
		return 0
	}

	return -1
}
