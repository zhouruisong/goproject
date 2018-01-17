package ruleMgr

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

type RuleInfo struct {
	id            int
	Name          string
	Start_time    int
	End_time      int
	Table_names   string
	Expire_time   int
	Max_del_speed int
	Batch_size    int
}

type RuleMgr struct {
	Logger    *log.Logger
	Dsn       string
	RuleInfos []RuleInfo
}

//从数据库中加载所有注册的设备信息
func (sMgr *RuleMgr) LoadRuleInfos() int {
	db, err := sql.Open("mysql", sMgr.Dsn)
	if err != nil {
		sMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	rows, err := db.Query("select name,start_time,end_time,table_names,expire_time,max_del_speed,batch_size from clean_rules")
	if err != nil {
		sMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		sMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	sMgr.RuleInfos = make([]RuleInfo, 0)

	var name string
	var start_time int
	var end_time int
	var table_names string
	var expire_time int
	var max_del_speed int
	var batch_size int
	for rows.Next() {
		err := rows.Scan(&name, &start_time, &end_time, &table_names, &expire_time, &max_del_speed, &batch_size)
		if err != nil {
			sMgr.Logger.Errorf("err:%v.\n", err)
			return -1
		}
		rl := RuleInfo{
			Name:          name,
			Start_time:    start_time,
			End_time:      end_time,
			Table_names:   table_names,
			Expire_time:   expire_time,
			Max_del_speed: max_del_speed,
			Batch_size:    batch_size,
		}
		sMgr.RuleInfos = append(sMgr.RuleInfos, rl)
	}

	return 0
}

const dsn_str = "root:110110@tcp(192.168.226.209:3306)/storage_center"

//向数据库注册新设备信息
func (sMgr *RuleMgr) addClnRule(sInfo *RuleInfo) int {
	db, err := sql.Open("mysql", dsn_str)
	if err != nil {
		sMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()
	stmtIns, err := db.Prepare("INSERT INTO device_info(name,host,cluster_name,groupid) VALUES( ?,?,?,? )")
	if err != nil {
		sMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(sInfo.Name)
	if err != nil {
		sMgr.Logger.Errorf("insert into mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("insert into mysql ok.")
	return 0
}

//从数据库删除设备信息
func (sMgr *RuleMgr) delClnRule(name string) int {
	db, err := sql.Open("mysql", dsn_str)
	if err != nil {
		sMgr.Logger.Errorf("err:%v.\n", err)
		return -1
	}
	defer db.Close()

	stmtIns, err := db.Prepare("delete from clean_rules where name = ?")
	if err != nil {
		sMgr.Logger.Errorf("Prepare failed, err:%v.\n", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec(name)
	if err != nil {
		sMgr.Logger.Errorf("del from mysql failed, err:%v.\n", err)
		return -1
	}

	fmt.Println("delete from mysql ok.")
	return 0
}

func NewRuleMgr(dsn string, lg *log.Logger) *RuleMgr {
	mgr := &RuleMgr{
		Logger:    lg,
		Dsn:       dsn,
		RuleInfos: make([]RuleInfo, 0),
	}
	return mgr
}
