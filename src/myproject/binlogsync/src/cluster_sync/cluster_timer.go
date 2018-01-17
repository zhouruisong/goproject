package cluster_sync

import (
	"../protocal"
	"time"
)

// 定时器发送失败的任务
func (sy *SyncMgr) TimerSendFailedTask() {
	sy.Logger.Infof("start TimerSendFailedTask")
	for {
		select {
		//定时任务取失败文件内容，重发到上传机
		case <-time.After(time.Second * time.Duration(sy.Interval)):
			sy.SendFailedTasks()
		}
	}
}

func (sy *SyncMgr) SendFailedTasks() {
	//获取所有失败的任务
	IdMap := sy.pBin.GetLastOkIdFileMap()
	for k, _ := range IdMap {
		ret, data := sy.pSql.SelectFailedInfo(k)
		if ret != 0 {
			sy.Logger.Errorf("SelectFailedInfo failed ret: %+v, table: %+v",
				ret, k)
			return
		}

		for _, item := range data {
			msg := &protocal.MsgMysqlBody{
				TableName: k,
				Data:      item,
			}

			sy.Logger.Infof("send task in table: %+v, taskid: %+v",
				item.TaskID, k)

			// 迁移数据到备份集群, 失败任务不会重复统计
			if sy.SyncData(msg, false) == 0 {
				// 迁移成功，删除失败任务表中的数据
				if sy.pSql.DeleteFailedTask(item.TaskID, k) != 0 {
					sy.Logger.Errorf("DeleteFailedTask failed taskid: %+v, table: %+v",
						item.TaskID, k)
				}
			}
		}
	}

	return
}
