package mysqlmgr

import (
	"../protocal"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
)

// 接收DB同步过来的内容，插入对应的live_master表中
func (mgr *MysqlMgr) MysqlReceive(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		mgr.Logger.Errorf("ReadAll failed. %v", err)
	}

	var r protocal.MsgInsertRet
	rt := mgr.handlerdbinsert(buf)

	if rt == 0 {
		r.Errno = rt
		r.Errmsg = "ok"
	} else {
		r.Errno = rt
		r.Errmsg = "failed"
	}

	b, err := json.Marshal(&r)
	if err != nil {
		mgr.Logger.Errorf("Marshal failed. %v", err)
	}

	res.Write(b) // HTTP 200
}

// 处理函数
func (mgr *MysqlMgr) handlerdbinsert(buf []byte) int {
	if len(buf) == 0 {
		mgr.Logger.Errorf("buf len = 0")
		return -1
	}

	var q protocal.DbEventInfo
	err := json.Unmarshal(buf, &q)
	if err != nil {
		mgr.Logger.Errorf("Error: cannot decode req body %v", err)
		return -1
	}

	mgr.Logger.Infof("handlerdbinsert receive data: %+v", q.DbData)

	ret := mgr.InsertMultiStreamInfos(q.DbData, q.TableName)
	if ret != 0 {
		mgr.Logger.Errorf("InsertMultiStreamInfos failed")
		return -1
	}

	return 0
}

func (mgr *MysqlMgr) GetLastTaskId(info protocal.DbInfo, tablename string) int {
	insertsql := "INSERT INTO " + "live_master." + tablename + " (task_id,file_name,file_type,file_size,domain,status, " +
		"action,md5_type,dname_md5,source_url,transcoding_url,file_md5,index_md5,head_md5,expiry_time, " +
		"create_time,exec_time,cb_url,ff_uri,task_branch_status,local_server_dir,ts_url,type,transcoding_info,is_backup) " +
		"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	start := time.Now()
	//Begin函数内部会去获取连接
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}

	defer stmtIns.Close()

	// 源文件默认为0，备份文件该值为1
	_, err = stmtIns.Exec(info.TaskId, info.FileName, info.FileType, info.FileSize, info.Domain, info.Status,
		info.Action, info.Md5Type, info.DnameMd5, info.SourceUrl, info.TransCodingUrl, info.FileMd5,
		info.IndexMd5, info.HeadMd5, info.ExpiryTime, info.CreateTime, info.ExecTime, info.CbUrl,
		info.FfUri, info.TaskBranchStatus, info.LocalServerDir, info.TsUrl, info.Type, info.TransCodingInfo, info.IsBackup)

	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

	end := time.Now()
	mgr.Logger.Infof("insert ok total time: %v", end.Sub(start).Seconds())

	return 0
}

func (mgr *MysqlMgr) InsertMultiStreamInfos(info protocal.DbInfo, tablename string) int {
	insertsql := "INSERT INTO " + "live_master." + tablename + " (task_id,file_name,file_type,file_size,domain,status, " +
		"action,md5_type,dname_md5,source_url,transcoding_url,file_md5,index_md5,head_md5,expiry_time, " +
		"create_time,exec_time,cb_url,ff_uri,task_branch_status,local_server_dir,ts_url,type,transcoding_info,is_backup) " +
		"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

//	start := time.Now()
	//Begin函数内部会去获取连接
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}

	defer stmtIns.Close()

	// 源文件默认为0，备份文件该值为1
	_, err = stmtIns.Exec(info.TaskId, info.FileName, info.FileType, info.FileSize, info.Domain, info.Status,
		info.Action, info.Md5Type, info.DnameMd5, info.SourceUrl, info.TransCodingUrl, info.FileMd5,
		info.IndexMd5, info.HeadMd5, info.ExpiryTime, info.CreateTime, info.ExecTime, info.CbUrl,
		info.FfUri, info.TaskBranchStatus, info.LocalServerDir, info.TsUrl, info.Type, info.TransCodingInfo, info.IsBackup)

	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

//	end := time.Now()

	return 0
}

func (mgr *MysqlMgr) InsertMultiStreamInfosTest(info protocal.DbInfo, tablename string) int {
	insertsql := "INSERT INTO " + "live_master." + tablename + " (task_id,file_name,file_type,file_size,domain,status, " +
		"action,md5_type,dname_md5,source_url,transcoding_url,file_md5,index_md5,head_md5,expiry_time, " +
		"create_time,exec_time,cb_url,ff_uri,task_branch_status,local_server_dir,ts_url,type,transcoding_info,is_backup) " +
		"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	start := time.Now()
	//Begin函数内部会去获取连接
	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}

	defer stmtIns.Close()

	// 源文件默认为0，备份文件该值为1
	_, err = stmtIns.Exec(info.TaskId, info.FileName, info.FileType, info.FileSize, info.Domain, info.Status,
		info.Action, info.Md5Type, info.DnameMd5, info.SourceUrl, info.TransCodingUrl, info.FileMd5,
		info.IndexMd5, info.HeadMd5, info.ExpiryTime, info.CreateTime, info.ExecTime, info.CbUrl,
		info.FfUri, info.TaskBranchStatus, info.LocalServerDir, info.TsUrl, info.Type, info.TransCodingInfo, info.IsBackup)

	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

	end := time.Now()
	mgr.Logger.Infof("insert ok total time: %v", end.Sub(start).Seconds())

	return 0
}