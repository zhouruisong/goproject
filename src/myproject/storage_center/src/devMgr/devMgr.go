package devMgr

//package main

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

type DevInfo struct {
	Id           int
	Name         string
	Host         string
	Ip           string
	Cluster_name string
	Groupid      string
}

type DevMgr struct {
	Logger       *log.Logger
	Dsn          string
	DevInfos     []DevInfo
	ClusterInfos []ClusterInfo
}

type ClusterInfo struct {
	Id   int
	Name string
	Host string
}

//从数据库中加载所有注册的设备信息
func (dMgr *DevMgr) LoadClusterInfos() int {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	rows, err := db.Query("select name, mysql_host from cluster_info")
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}

	dMgr.ClusterInfos = make([]ClusterInfo, 0)
	var name string
	var host string
	for rows.Next() {
		err := rows.Scan(&name, &host)
		if err != nil {
			dMgr.Logger.Errorf("err:%v.\n", err)
			return -1
		}
		cinfo := ClusterInfo{
			Name: name,
			Host: host,
		}
		dMgr.ClusterInfos = append(dMgr.ClusterInfos, cinfo)
	}

	return 0
}

//从数据库中加载所有注册的设备信息
func (dMgr *DevMgr) LoadDevInfos() int {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	rows, err := db.Query("select name, host, ip, cluster_name, groupid from device_info")
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}

	dMgr.DevInfos = make([]DevInfo, 0)
	var name string
	var host string
	var ip string
	var cluster_name string
	var groupid string
	for rows.Next() {
		err := rows.Scan(&name, &host, &ip, &cluster_name, &groupid)
		if err != nil {
			dMgr.Logger.Errorf("err:%v.\n", err)
			return -1
		}
		dinfo := DevInfo{
			Name:         name,
			Host:         host,
			Ip:           ip,
			Cluster_name: cluster_name,
			Groupid:      groupid,
		}
		dMgr.DevInfos = append(dMgr.DevInfos, dinfo)
	}
	return 0
}

//向数据库添加集群信息
func (dMgr *DevMgr) registerCluster(cInfo *ClusterInfo) int {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()
	stmtIns, err := db.Prepare("INSERT INTO cluster_info(name,mysql_host) VALUES( ?,? )")
	if err != nil {
		dMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(cInfo.Name, cInfo.Host)
	if err != nil {
		dMgr.Logger.Errorf("insert into mysql failed, err:%v.\n", err)
		return -1
	}

	return 0
}

const dsn_str = "root:110110@tcp(192.168.226.209:3306)/storage_center"

//向数据库注册新设备信息
func (dMgr *DevMgr) registerDev(dInfo *DevInfo) int {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()
	stmtIns, err := db.Prepare("INSERT INTO device_info(name,host,cluster_name,groupid) VALUES( ?,?,?,? )")
	if err != nil {
		dMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(dInfo.Name, dInfo.Host, dInfo.Cluster_name, dInfo.Groupid)
	if err != nil {
		dMgr.Logger.Errorf("insert into mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("insert into mysql ok.")
	return 0
}

//从数据库删除集群信息
func (dMgr *DevMgr) deleteCluster(name string) int {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	stmtIns, err := db.Prepare("delete from cluster_info where name = ?")
	if err != nil {
		dMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(name)
	if err != nil {
		dMgr.Logger.Errorf("del from mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("delete from mysql ok.")
	return 0
}

//从数据库删除设备信息
func (dMgr *DevMgr) delDev(name string) int {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	stmtIns, err := db.Prepare("delete from device_info where name = ?")
	if err != nil {
		dMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(name)
	if err != nil {
		dMgr.Logger.Errorf("del from mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("delete from mysql ok.")
	return 0
}

//查询指定ip的设备所在的组信息
func (dMgr *DevMgr) getDevGroupInfo(h string) (ret int, res []DevInfo) {
	db, err := sql.Open("mysql", dMgr.Dsn)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1, nil
	}
	defer db.Close()

	sql_str := fmt.Sprintf("select name, host, ip, cluster_name, groupid from device_info where cluster_name = \"%s\"", h)

	dMgr.Logger.Infof("Get request:%v", sql_str)
	rows, err := db.Query(sql_str)
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1, nil
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		dMgr.Logger.Errorf("err:%v.\n", err)
		return -1, nil
	}

	dMgr.DevInfos = make([]DevInfo, 0)
	var name string
	var host string
	var ip string
	var cluster_name string
	var groupid string
	for rows.Next() {
		err := rows.Scan(&name, &host, &ip, &cluster_name, &groupid)
		if err != nil {
			dMgr.Logger.Errorf("err:%v.\n", err)
			return -1, nil
		}
		dinfo := DevInfo{
			Name:         name,
			Host:         host,
			Ip:           ip,
			Cluster_name: cluster_name,
			Groupid:      groupid,
		}
		res = append(res, dinfo)
	}
	return 0, res
}

func NewDevMgr(dsn string, lg *log.Logger) *DevMgr {
	mgr := &DevMgr{
		Dsn:          dsn,
		Logger:       lg,
		DevInfos:     make([]DevInfo, 0),
		ClusterInfos: make([]ClusterInfo, 0),
	}
	return mgr
}
