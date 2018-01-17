package mysqlmgr

import (
	"../protocal"
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

var (
	db         *sql.DB
	db_task    *sql.DB
	g_dsn      = ""
	g_dsn_task = ""
)

type MysqlMgr struct {
	Logger     *log.Logger
	FdfsBackup string
}

func NewMysqlMgr(dsn string, dsn_t string, fdfsip string, lg *log.Logger) *MysqlMgr {
	mgr := &MysqlMgr{
		Logger:     lg,
		FdfsBackup: fdfsip,
	}

	g_dsn = dsn
	g_dsn_task = dsn_t

	err := mgr.initGdsn()
	if err != nil {
		mgr.Logger.Errorf("mgr.initGdsn failed")
		return nil
	}

	err = mgr.initGdsnTask()
	if err != nil {
		mgr.Logger.Errorf("mgr.initGdsnTask failed")
		return nil
	}

	mgr.Logger.Infof("NewMysqlMgr ok")
	return mgr
}

func (mgr *MysqlMgr) initGdsn() error {
	sqldb, err := sql.Open("mysql", g_dsn)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return err
	}

	db = sqldb
	return nil
}

func (mgr *MysqlMgr) initGdsnTask() error {
	sqldb, err := sql.Open("mysql", g_dsn_task)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return err
	}

	db_task = sqldb

	return nil
}

func (mgr *MysqlMgr) CreateFailedTable(tablename string) int {
	create_sql := "CREATE TABLE if not exists " + "upload_task." + tablename +
		"(id int(11) unsigned NOT NULL AUTO_INCREMENT,rcv_id int(11) NOT NULL DEFAULT '0'," +
		"task_id char(32) NOT NULL DEFAULT '',file_name varchar(256) NOT NULL DEFAULT ''," +
		"file_type char(10) NOT NULL DEFAULT '0',file_size int(11) NOT NULL DEFAULT '0'," +
		"domain varchar(64) NOT NULL DEFAULT '',app varchar(128) NOT NULL DEFAULT ''," +
		"source_type char(6) NOT NULL DEFAULT '0',uri varchar(256) NOT NULL DEFAULT ''," +
		"cb_url varchar(256) NOT NULL,file_md5 char(32) NOT NULL DEFAULT ''," +
		"index_md5 char(32) NOT NULL DEFAULT '',head_md5 char(32) NOT NULL DEFAULT ''," +
		"expiry_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'," +
		"create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"is_backup tinyint(4) NOT NULL DEFAULT '0',PRIMARY KEY (`id`),KEY `task_id` (`task_id`) " +
		") ENGINE=MyISAM DEFAULT CHARSET=utf8;"

	//mgr.Logger.Infof("create_sql: %+v", create_sql)

	stmtIns, err := db.Prepare(create_sql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}

	defer stmtIns.Close()

	_, err = stmtIns.Exec()
	if err != nil {
		mgr.Logger.Errorf("create table %s failed, err:%v", tablename, err)
		return -1
	}

	return 0
}

func (mgr *MysqlMgr) InsertBackupUploadDB(info protocal.DbInfo, tablename string) int {
	insertsql := "INSERT INTO " + "live_master." + tablename + "(task_id,subtask_id,file_name," +
		"file_type,file_size,domain,app,source_type,uri,cb_url,file_md5,index_md5,head_md5," +
		"expiry_time,create_time,is_backup) " + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	//	start := time.Now()
	//Begin函数内部会去获取连接
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}

	defer stmtIns.Close()

	// 源文件默认为0，备份文件该值为1
	_, err = stmtIns.Exec(info.TaskID, info.SubtaskID, info.FileName, info.FileType, info.FileSize, info.Domain,
		info.App, info.SourceType, info.URI, info.CbURL, info.FileMd5, info.IndexMd5, info.HeadMd5,
		info.ExpiryTime, info.CreateTime, info.IsBackup)

	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

	//	end := time.Now()

	return 0
}

func (mgr *MysqlMgr) InsertFailedTask(info *protocal.DbInfo, tablename string) int {
	insertsql := "INSERT INTO " + "upload_task." + tablename + " (rcv_id,task_id,file_name, " +
		"file_type,file_size,domain,app,source_type,uri,cb_url,file_md5,index_md5,head_md5," +
		"expiry_time,create_time,is_backup) " + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	//	start := time.Now()
	//Begin函数内部会去获取连接
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}

	defer stmtIns.Close()

	// 源文件默认为0，备份文件该值为1
	_, err = stmtIns.Exec(info.ID, info.TaskID, info.FileName, info.FileType, info.FileSize, info.Domain,
		info.App, info.SourceType, info.URI, info.CbURL, info.FileMd5, info.IndexMd5, info.HeadMd5,
		info.ExpiryTime, info.CreateTime, info.IsBackup)

	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

	//	end := time.Now()
	//	mgr.Logger.Infof("insert ok total time: %v", end.Sub(start).Seconds())

	return 0
}

// 发送成功后，删除失败任务中的记录
func (mgr *MysqlMgr) DeleteFailedTask(taskid, tablename string) int {
	TableName := "upload_task." + tablename
	delete_sql := "delete from " + TableName + " where task_id = ?"

	stmtIns, err := db_task.Prepare(delete_sql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(taskid)
	if err != nil {
		mgr.Logger.Errorf("del from mysql failed, err:%v.\n", err)
		return -1
	}

	mgr.Logger.Infof("delete from %s ok task_id:%+v", tablename, taskid)
	return 0
}

// 搜索失败的任务，进行发送
func (mgr *MysqlMgr) SelectFailedInfo(tablename string) (int, []protocal.DbInfo) {
	var info []protocal.DbInfo
	var querysql string
	TableName := "upload_task." + tablename
	querysql = fmt.Sprintf("SELECT * FROM %s", TableName)

	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return -1, info
	}

	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return -1, info
	}

	var id int
	var taskid string
	var subtaskid string
	var filename string
	var filetype string
	var filesize int
	var domain string
	var app string
	var sourcetype string
	var uri string
	var cburl string
	var filemd5 string
	var indexmd5 string
	var headmd5 string
	var expirytime string
	var createtime string
	var isbackup int

	for rows.Next() {
		err := rows.Scan(&id, &taskid, &subtaskid, &filename, &filetype, &filesize,
			&domain, &app, &sourcetype, &uri, &cburl, &filemd5, &indexmd5, &headmd5,
			&expirytime, &createtime, &isbackup)

		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return -1, info
		}

		data := protocal.DbInfo{
			ID:         id,
			TaskID:     taskid,
			SubtaskID:  subtaskid,
			FileName:   filename,
			FileType:   filetype,
			FileSize:   filesize,
			Domain:     domain,
			App:        app,
			SourceType: sourcetype,
			URI:        uri,
			CbURL:      cburl,
			FileMd5:    filemd5,
			IndexMd5:   indexmd5,
			HeadMd5:    headmd5,
			ExpiryTime: expirytime,
			CreateTime: createtime,
			IsBackup:   isbackup,
		}
		info = append(info, data)
	}

	//	mgr.Logger.Infof("tablename:%+v, startId:%+v, len:%v", tablename, startId, len(info))
	return 0, info
}

// 搜索所有大于startId的数据，进行发送
func (mgr *MysqlMgr) SelectGreaterThanId(tablename string, startId int) (int, []protocal.DbInfo) {
	var info []protocal.DbInfo
	var querysql string
	TableName := "live_master." + tablename
	querysql = fmt.Sprintf("SELECT * FROM %s WHERE %s.id > %d",
		TableName, TableName, startId)

	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return -1, info
	}

	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return -1, info
	}

	var id int
	var taskid string
	var subtaskid string
	var filename string
	var filetype string
	var filesize int
	var domain string
	var app string
	var sourcetype string
	var uri string
	var cburl string
	var filemd5 string
	var indexmd5 string
	var headmd5 string
	var expirytime string
	var createtime string
	var isbackup int

	for rows.Next() {
		err := rows.Scan(&id, &taskid, &subtaskid, &filename, &filetype, &filesize,
			&domain, &app, &sourcetype, &uri, &cburl, &filemd5, &indexmd5, &headmd5,
			&expirytime, &createtime, &isbackup)

		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return -1, info
		}

		data := protocal.DbInfo{
			ID:         id,
			TaskID:     taskid,
			SubtaskID:  subtaskid,
			FileName:   filename,
			FileType:   filetype,
			FileSize:   filesize,
			Domain:     domain,
			App:        app,
			SourceType: sourcetype,
			URI:        uri,
			CbURL:      cburl,
			FileMd5:    filemd5,
			IndexMd5:   indexmd5,
			HeadMd5:    headmd5,
			ExpiryTime: expirytime,
			CreateTime: createtime,
			IsBackup:   isbackup,
		}
		info = append(info, data)
	}

	mgr.Logger.Infof("tablename:%+v, startId:%+v, len:%v", tablename, startId, len(info))
	return 0, info
}

// 搜索所有live_master库下的表
func (mgr *MysqlMgr) SelectMapTable() ([]string, error) {
	var tablename []string

	querysql := "SELECT table_suffix FROM live_master.t_vhost2tbname_map"
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

		if len(table) != 0 {
			table = "t_live_fcup_source_" + table
			tablename = append(tablename, table)
		}
	}

	mgr.Logger.Infof("tablename:%v, ", tablename)
	return tablename, nil
}

// 搜索所有live_master库下的表
func (mgr *MysqlMgr) SelectTableName() ([]string, error) {
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
