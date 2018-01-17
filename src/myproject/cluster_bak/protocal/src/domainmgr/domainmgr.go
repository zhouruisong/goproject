package domainmgr

import (
	"fmt"
	// "strings"
	"database/sql"
	"time"
//	 "reflect"
	"github.com/2tvenom/myreplication"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

var (
	host        = "localhost"
	port        = 3306
	username    = "root"
	password    = "123456"
	server_Id   = 1
	eachNum     = 100
	dbconns     = 200
	dbidleconns = 100
	db          *sql.DB
)

type CheckInfo struct {
	Tablename string
	Info      StreamInfo
}

type InsertInfo struct {
	TaskId       string
	TaskServer   string
	FileName     string
	FileType     int
	FileSize     int
	FileMd5      string
	Domain       string
	App          string
	Stream       string
	Step         int
	PublishTime  int64
	NotifyUrl    string
	NotifyReturn string
	Status       int
	ExpireTime   string
	CreateTime   string
	UpdateTime   string
	EndTime      string
	NotifyTime   string
}

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

type BinLogMgr struct {
	Logger    *log.Logger
	EventChan chan *CheckInfo
}

type StreamMgr struct {
	Logger          *log.Logger
	Dsn             string
	MysqlBackupList string
	StreamInfos     []StreamInfo
}

func NewBinLogMgr(host_ string, port_ int, username_ string, password_ string,
	serverId_ int, each int, dbconns int, dbidle int, lg *log.Logger) *BinLogMgr {

	my := &BinLogMgr{
		Logger:    lg,
		EventChan: make(chan *CheckInfo, 1000),
	}
	my.setParamter(host_, port_, username_, password_, serverId_, each, dbconns, dbidle)
	my.Logger.Infof("NewBinLogMgr ok")
	return my
}

func (mgr *BinLogMgr) setParamter(host_ string, port_ int, username_ string,
	password_ string, serverId_ int, each int, dbconn int, dbidle int) {

	host = host_
	port = port_
	username = username_
	password = password_
	server_Id = serverId_
	eachNum = each
	dbconns = dbconn
	dbidleconns = dbidle
}

func (mgr *BinLogMgr) Read() *CheckInfo {
	select {
		case info := <-mgr.EventChan:
			mgr.Logger.Infof("Read to channel successful: %+v", info)
			return info
		case <-time.After(time.Second * 2):
//			mgr.Logger.Errorf("Read to channel timeout")
			return nil
	}
	return nil
}

func (mgr *BinLogMgr) Write(info *CheckInfo) int {
	select {
		case mgr.EventChan <- info:
			// mgr.Logger.Infof("write to channel successful: %+v", info)
			return 0
		case <-time.After(time.Second * 2):
			mgr.Logger.Errorf("write to channel timeout: %+v", info)
	}
	return 0
}

func (mgr *BinLogMgr) ReflectUint32(ty interface{}) (uint32, bool) {
	if value, ok := ty.(uint32); ok {
		return value, true
	}

	return 0, false
}

func (mgr *BinLogMgr) ReflectUint8(ty interface{}) (uint8, bool) {
	if value, ok := ty.(uint8); ok {
		return value, true
	}

	return 0, false
}

func (mgr *BinLogMgr) ReflectUint64(ty interface{}) (uint64, bool) {
	if value, ok := ty.(uint64); ok {
		return value, true
	}

	return 0, false
}

func (mgr *BinLogMgr) ReflectString(ty interface{}) (string, bool) {
	if value, ok := ty.(string); ok {
		return value, true
	}

	return "", false
}

func (mgr *BinLogMgr) ReflectTime(ty interface{}) (string, bool) {
	if value, ok := ty.(time.Time); ok {
		return value.Format("2006-01-02 15:04:05"), true
	}

	return "", false
}

// 根据类型获取对应的
func (mgr *BinLogMgr) ReflectData(j int, data *CheckInfo, col interface{}) {
	switch j {
		case 0:
			data.Info.Id, _ = mgr.ReflectUint32(col)
		case 1:
			data.Info.TaskId, _ = mgr.ReflectString(col)
		case 2:
			data.Info.TaskServer, _ = mgr.ReflectString(col)
		case 3:
			data.Info.FileName, _ = mgr.ReflectString(col)
		case 4:
			data.Info.FileType, _ = mgr.ReflectUint8(col)
			mgr.Logger.Infof("ReflectData FileType: %+v", data.Info.FileType)
		case 5:
			data.Info.FileSize, _ = mgr.ReflectUint32(col)
		case 6:
			data.Info.FileMd5, _ = mgr.ReflectString(col)
		case 7:
			data.Info.Domain, _ = mgr.ReflectString(col)
		case 8:
			data.Info.App, _ = mgr.ReflectString(col)
		case 9:
			data.Info.Stream, _ = mgr.ReflectString(col)
		case 10:
			data.Info.Step, _ = mgr.ReflectUint8(col)
		case 11:
			data.Info.PublishTime, _ = mgr.ReflectUint64(col)
		case 12:
			data.Info.NotifyUrl, _ = mgr.ReflectString(col)
		case 13:
			data.Info.NotifyReturn, _ = mgr.ReflectString(col)
		case 14:
			data.Info.Status, _ = mgr.ReflectUint8(col)
		case 15:
			data.Info.ExpireTime, _ = mgr.ReflectTime(col)
		case 16:
			data.Info.CreateTime, _ = mgr.ReflectTime(col)
		case 17:
			data.Info.UpdateTime, _ = mgr.ReflectTime(col)
		case 18:
			data.Info.EndTime, _ = mgr.ReflectTime(col)
		case 19:
			data.Info.NotifyTime, _ = mgr.ReflectTime(col)
		default:
			mgr.Logger.Errorf("wrong j: %+v", j)
	}
}

func (mgr *BinLogMgr) RunMoniterMysql() {
	newConnection := myreplication.NewConnection()
	serverId := uint32(1)
	err := newConnection.ConnectAndAuth(host, port, username, password)

	if err != nil {
		panic("Client not connected and not autentificate to master server error:" + err.Error())
	}
	//Get position and file name
	pos, filename, err := newConnection.GetMasterStatus()
	
	println("pos: ", pos)
	println("filename: ", filename)
	
	if err != nil {
		panic("Master status fail: " + err.Error())
	}
	
	el, err := newConnection.StartBinlogDump(pos, filename, serverId)
	
	if err != nil {
		panic("Cant start bin log: " + err.Error())
	}
	events := el.GetEventChan()
	go func() {
		for {
			event := <-events

			switch e := event.(type) {
			case *myreplication.QueryEvent:
				//Output query event
				println(e.GetQuery())
			case *myreplication.IntVarEvent:
				//Output last insert_id  if statement based replication
				println(e.GetValue())
			case *myreplication.WriteEvent:
				//Output Write (insert) event
				println("Write", e.GetTable())
//				tablename := e.GetTable()
//				mgr.Logger.Infof("Write %v", tablename)
				//Rows loop
				for _, row := range e.GetRows() {
					
					println(fmt.Sprintf("row : %+v", row[0]))
					println(fmt.Sprintf("row : %+v", row[1]))
					println(fmt.Sprintf("row : %+v", row[2]))
					println(fmt.Sprintf("row : %+v", row[3]))
					println(fmt.Sprintf("row : %+v", row[4]))
					
					//Columns loop
					var data CheckInfo
//					for j, col := range row {						
						// Output row number, column number, column type and column value
//						println(fmt.Sprintf("%v %d %d %d %v", reflect.TypeOf(col.GetValue()), 
//								i, j, col.GetType(), col.GetValue()))	
//						mgr.ReflectData(j, &data, col.GetValue())
//					}
					
//					data.Tablename = tablename
					
					// 每次插入一条数据，最后一条会出现插入空的一条记录，跳过这条无效记录
//					if len(data.Info.TaskId) == 0 {
//						mgr.Logger.Infof("TaskId len: %v", len(data.Info.TaskId))
//						break
//					}
//					mgr.Logger.Infof("FileType: %+v", data.Info.FileType)
//					// filetype = 1表示是备份过来的数据，这里不需要在同步到备份服务器
//					if data.Info.FileType == 1 {
//						mgr.Logger.Infof("FileType: %+v", data.Info.FileType)
//						break
//					}
					
//					if mgr.Write(&data) != 0 {
//						mgr.Logger.Errorf("write to channel fail: %+v", data)
//					}
					mgr.Logger.Infof("write to channel successful: %+v", data)
					break
				}
			case *myreplication.DeleteEvent:
				//Output delete event
				println("Delete", e.GetTable())
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *myreplication.UpdateEvent:
				//Output update event
				println("Update", e.GetTable())
				//Output old data before update
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
				//Output new
				for i, row := range e.GetNewRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			default:
			}
		}
	}()
	err = el.Start()
	println(err.Error())
}

func NewStreamMgr(dsn string, iplist string, lg *log.Logger) *StreamMgr {
	mgr := &StreamMgr{
		Logger:          lg,
		Dsn:             dsn,
		MysqlBackupList: iplist,
	}

	err := mgr.init()
	if err != nil {
		mgr.Logger.Infof("mgr.init failed")
		return nil
	}
	mgr.Logger.Infof("NewStreamMgr ok")
	return mgr
}

func (mgr *StreamMgr) init() error {
	var err error
	db, err = sql.Open("mysql", mgr.Dsn)
	if err != nil {
		mgr.Logger.Errorf("err:%v.\n", err)
		return err
	}

	db.SetMaxOpenConns(dbconns)
	db.SetMaxIdleConns(dbidleconns)
	db.Ping()
	return nil
}

// 搜索所有live_master库下的表，为全量同步�?
func (mgr *StreamMgr) SelectTableName() ([]string, error) {
	var tablename []string

	querysql := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'live_master'"
	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return tablename, err
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return tablename, err
	}

	var table string
	for rows.Next() {
		err := rows.Scan(&table)
		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return tablename, err
		}
		tablename = append(tablename, table)
	}

	mgr.Logger.Infof("tablename:%v, ", tablename)
	return tablename, nil
}

// 查询文件是否已经在数据库中存在，
func (mgr *StreamMgr) SelectDataExist(taskId, tablename string) int {
	count := 0
	querysql := fmt.Sprintf("select count(1) from live_master.%s where %s.task_id = \"%s\" ",
		tablename, tablename, taskId)

	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return 1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return 1
	}

	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return 1
		}
	}

	mgr.Logger.Infof("taskId:%v, count: %v", taskId, count)
	return count
}

// 增量同步，启动的时候按照配置文件加载内容，分批发送给备份服务�?
func (mgr *StreamMgr) LoadStreamInfos(beginIndex int, tablename string) ([]StreamInfo, int) {
//	mgr.Logger.Infof("start LoadStreamInfos")
	tmpNum := beginIndex * eachNum

	var returnInfo []StreamInfo
	querysql := "select id,task_id,task_server,file_name,file_type,file_size,file_md5,domain,app,stream,step," +
		"publish_time,notify_url,notify_return,status,expiry_time,create_time,update_time,end_time,notify_time from " +
		tablename + " where file_type=0 " + "limit " + strconv.Itoa(tmpNum) + "," + strconv.Itoa(eachNum)
		
//	mgr.Logger.Infof("querysql: %+v", querysql)
	
	// only loda upload complete
	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return returnInfo, -1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return returnInfo, -1
	}

	var id uint32
	var taskId string
	var taskServer string
	var fileName string
	var fileType uint8
	var fileSize uint32
	var fileMd5 string
	var domain string
	var app string
	var stream string
	var step uint8
	var publishTime uint64
	var notifyUrl string
	var notifyReturn string
	var status uint8
	var expireTime string
	var createTime string
	var updateTime string
	var endTime string
	var notifyTime string

	for rows.Next() {
		err := rows.Scan(&id, &taskId, &taskServer, &fileName, &fileType, &fileSize, &fileMd5,
			&domain, &app, &stream, &step, &publishTime, &notifyUrl, &notifyReturn, &status,
			&expireTime, &createTime, &updateTime, &endTime, &notifyTime)

		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return returnInfo, -1
		}

		info := StreamInfo{
			Id:           id,
			TaskId:       taskId,
			TaskServer:   taskServer,
			FileName:     fileName,
			FileType:     fileType,
			FileSize:     fileSize,
			FileMd5:      fileMd5,
			Domain:       domain,
			App:          app,
			Stream:       stream,
			Step:         step,
			PublishTime:  publishTime,
			NotifyUrl:    notifyUrl,
			NotifyReturn: notifyReturn,
			Status:       status,
			ExpireTime:   expireTime,
			CreateTime:   createTime,
			UpdateTime:   updateTime,
			EndTime:      endTime,
			NotifyTime:   notifyTime,
		}
		returnInfo = append(returnInfo, info)
	}

	mgr.Logger.Infof("LoadStreamInfos len: %+v", len(returnInfo))
	if len(returnInfo) == 0 {
		return returnInfo, -1
	}
	return returnInfo, 0
}

func (mgr *StreamMgr) InsertStreamInfos(i int) int {
	mgr.Logger.Infof("start InsertStreamInfos")
	insertsql := "INSERT INTO t_live2odv2_kuwo" +
		"(task_id,task_server,file_name,file_type,file_size,file_md5,domain,app,stream,step," +
		"publish_time,notify_url,notify_return,status,expiry_time,create_time,update_time,end_time,notify_time) " +
		"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
		
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}
	defer stmtIns.Close()
	
	taskid := fmt.Sprintf("%s%08d", "83f95bd17727e99e286507b2", i)

	mgr.Logger.Infof("taskid: %v", taskid)
	
	_, err = stmtIns.Exec(taskid, "", "/voicelive/219705672_preprocess-1477649359414.m3u8", 0, 0, "", "push.xycdn.kuwo.cn", "voicelive", "219705672_preprocess", 3, 1477649359414, "http://127.0.0.1:8080/accept_test.php", "string(279) \"{\"task_id\":\"8e9addd82febf91d0fffead1760b507a\",\"domain\":\"push.xycdn.kuwo.cn\",\"app\":\"voicelive\",\"stream\":\"219705672_preprocess\",\"tag\":\"/voicelive/219705672_preprocess-1477649359414.m3u8\",\"vod_url\":\"test.com\",\"vod_md5\":\"\",\"vod_size\":\"0\",\"vod_star", 1, "0000-00-00 00:00:00", "2016-10-28 10:09:19", "0000-00-00 00:00:00", "0000-00-00 00:00:00", "2016-10-28 10:09:29")
	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

	mgr.Logger.Infof("insert into mysql ok")
	return 0
}

func (mgr *StreamMgr) InsertMultiStreamInfos(info []StreamInfo, tablename string) int {
	datalen := len(info)
	if datalen == 0 {
		mgr.Logger.Errorf("datalen = 0")
		return -1
	}

	insertsql := "INSERT INTO " + tablename + " (task_id,task_server,file_name,file_type,file_size,file_md5,domain,app,stream,step," +
		"publish_time,notify_url,notify_return,status,expiry_time,create_time,update_time,end_time,notify_time) " +
		"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	start := time.Now()
	//Begin函数内部会去获取连接
//	tx, err := db.Begin()
//	if err != nil {
//		mgr.Logger.Errorf("db.Begin(), err:%v", err)
//		return -1
//	}
	
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}
	defer stmtIns.Close()

	for i := 0; i < datalen; i++ {
		// 源文件默认为0，备份文件该值为1
		info[i].FileType = 1
		//每次循环用的都是tx内部的连接，没有新建连接，效率高
		_, err = stmtIns.Exec(info[i].TaskId, info[i].TaskServer, info[i].FileName, info[i].FileType, info[i].FileSize,
			info[i].FileMd5, info[i].Domain, info[i].App, info[i].Stream, info[i].Step, info[i].PublishTime, info[i].NotifyUrl,
			info[i].NotifyReturn, info[i].Status, info[i].ExpireTime, info[i].CreateTime, info[i].UpdateTime, info[i].EndTime, info[i].NotifyTime)
		
		if err != nil {
			mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
			return -1
		}
	}

//	stmtIns, err := tx.Prepare(insertsql)
//	if err != nil {
//		mgr.Logger.Errorf("Prepare failed, err:%v", err)
//		return -1
//	}
//	defer stmtIns.Close()
//
//	for i := 0; i < datalen; i++ {
//		count := mgr.SelectDataExist(info[i].TaskId, tablename)
//		if count != 0 {
//			mgr.Logger.Errorf("taskid:%v exist in %v", info[i].TaskId, tablename)
//			continue
//		}
		
		// 源文件默认为0，备份文件该值为1
//		info[i].FileType = 1
//		//每次循环用的都是tx内部的连接，没有新建连接，效率高
//		stmtIns.Exec(info[i].TaskId, info[i].TaskServer, info[i].FileName, info[i].FileType, info[i].FileSize,
//			info[i].FileMd5, info[i].Domain, info[i].App, info[i].Stream, info[i].Step, info[i].PublishTime, info[i].NotifyUrl,
//			info[i].NotifyReturn, info[i].Status, info[i].ExpireTime, info[i].CreateTime, info[i].UpdateTime, info[i].EndTime, info[i].NotifyTime)
//	}
//	//出异常回�?
//	defer tx.Rollback()
//
//	//最后释放tx内部的连�?
//	tx.Commit()

	end := time.Now()
	mgr.Logger.Infof("insert ok total time: %v", end.Sub(start).Seconds())

	return 0
}
