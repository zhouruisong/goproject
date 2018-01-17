package cluster_sync

import (
	"../protocal"
)

//  初始化最后成功接收id缓存，每个表保存一个最后成功接收id
func (sy *SyncMgr) InitCache() error {
	return sy.pBin.FileInfoRead()
}

// 启动时获取大于文件记录的id的所有任务
func (sy *SyncMgr) SendOldSyncTask() {
	sy.Logger.Infof("start SendBiggerLastIdTask")
	IdMap := sy.pBin.GetLastOkIdFileMap()

	//查询map表，查询有几个用户结果表
	tablelist, err := sy.pSql.SelectMapTable()
	if err != nil {
		sy.Logger.Errorf("SelectMapTable err:%+v", err)
		return
	}
	
	length := len(tablelist)
	if length == 0 {
		sy.Logger.Errorf("length == 0")
		return
	}
	
	var id int
	for i := 0; i < length; i++ {
		if v, ok := IdMap[tablelist[i]]; ok {
			id = v
		} else {
			id = 0
		}
		//查询数据库获取大于ID的所有数据，重新进行数据迁移
		ret, data := sy.pSql.SelectGreaterThanId(tablelist[i], id)
		if ret != 0 {
			continue
		}
		
		for _, item := range data {
			msg := &protocal.MsgMysqlBody{
				TableName: tablelist[i],
				Data:      item,
			}
			// 迁移数据到备份集群
			sy.SyncData(msg, true)
			// 发送结构文件到es，用于统计数据使用
			ret := sy.sendToEs(&msg.Data)
			if ret != 0 {
				sy.Logger.Errorf("sendToEs failed taskid:%+v", msg.Data.TaskID)
			}
		}
	}
}
