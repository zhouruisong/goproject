package devMgr

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

type DevScreenInfo struct {
	Name string
	Host string
}

type DevScreenMgr struct {
	Logger   *log.Logger
	Dsn      string
	DevInfos []DevScreenInfo
}

//从数据库中加载所有注册的设备信息
func (screenMgr *DevScreenMgr) LoadScreenDevInfos() int {
	db, err := sql.Open("mysql", screenMgr.Dsn)
	if err != nil {
		screenMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	rows, err := db.Query("select name, host from screenshot_device_info where status = 0")
	if err != nil {
		screenMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		screenMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}

	screenMgr.DevInfos = make([]DevScreenInfo, 0)
	var name string
	var host string
	for rows.Next() {
		err := rows.Scan(&name, &host)
		if err != nil {
			screenMgr.Logger.Errorf("err:%v.\n", err)
			return -1
		}

		dinfo := DevScreenInfo{
			Name: name,
			Host: host,
		}
		screenMgr.DevInfos = append(screenMgr.DevInfos, dinfo)
	}
	return 0
}

//向数据库注册新设备信息
func (screenMgr *DevScreenMgr) registerScreenDev(dInfo *DevScreenInfo) int {
	db, err := sql.Open("mysql", screenMgr.Dsn)
	if err != nil {
		screenMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()
	stmtIns, err := db.Prepare("INSERT INTO screenshot_device_info(name, host, status) VALUES( ?,?,? )")
	if err != nil {
		screenMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(dInfo.Name, dInfo.Host)
	if err != nil {
		screenMgr.Logger.Errorf("insert into mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("insert into mysql ok.")
	return 0
}

//从数据库删除集群信息
func (screenMgr *DevScreenMgr) deleteScreenDev(name string) int {
	db, err := sql.Open("mysql", screenMgr.Dsn)
	if err != nil {
		screenMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	stmtIns, err := db.Prepare("delete from screenshot_device_info where name = ?")
	if err != nil {
		screenMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(name)
	if err != nil {
		screenMgr.Logger.Errorf("del from mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("delete from mysql ok.")
	return 0
}

func NewScreenDevMgr(dsn string, lg *log.Logger) *DevScreenMgr {
	mgr := &DevScreenMgr{
		Dsn:      dsn,
		Logger:   lg,
		DevInfos: make([]DevScreenInfo, 0),
	}
	return mgr
}
