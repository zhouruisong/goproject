package mysqlmgr

import (
	"../protocal"
	"encoding/json"
)

// 处理函数
func (mgr *MysqlMgr) Handlerdbinsert(buf []byte) (int, string) {
	if len(buf) == 0 {
		return -1, "buf len = 0"
	}

	var q protocal.MsgMysqlBody
	err := json.Unmarshal(buf, &q)
	if err != nil {
		mgr.Logger.Errorf("Error: cannot decode req body %v", err)
		return -1, "Handlerdbinsert Unmarshal failed"
	}

	//	mgr.Logger.Infof("receive data: %+v", q.Data)

	ret := mgr.InsertBackupUploadDB(q.Data, q.TableName)
	if ret != 0 {
		mgr.Logger.Errorf("InsertMultiStreamInfos failed taskid:%+v", q.Data.TaskID)
		return -1, "InsertMultiStreamInfos failed"
	}

	return 0, "ok"
}
