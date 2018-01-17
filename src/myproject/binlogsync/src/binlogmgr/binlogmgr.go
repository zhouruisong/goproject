package binlogmgr

import (
	"../mysqlmgr"
	"../protocal"
	log "github.com/Sirupsen/logrus"
	"os"
	"time"
)

var (
	g_sync_part_time  = false
	g_lastidfile_path string

	g_FailedTaskFile  *os.File = nil
	g_LastOkIdFileMap          = make(map[string]*os.File)
	g_LastOkIdCache            = make(map[string]int)
)

type TimeInfo struct {
	hour   int
	minute int
	second int
}

type BinLogMgr struct {
	Logger     *log.Logger
	pSql       *mysqlmgr.MysqlMgr
	EventChan  chan *protocal.MsgMysqlBody
	start_time TimeInfo
	end_time   TimeInfo
}

func NewBinLogMgr(lastidfile string, psql *mysqlmgr.MysqlMgr, starttime string,
	endtime string, pipelen int, lg *log.Logger) *BinLogMgr {
	my := &BinLogMgr{
		Logger:    lg,
		pSql:      psql,
		EventChan: make(chan *protocal.MsgMysqlBody, pipelen),
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

	rt := GetSyncTime(my, starttime, endtime)
	if rt != true {
		my.Logger.Errorf("GetSyncTime failed")
		return nil
	}

	g_lastidfile_path = lastidfile

	my.Logger.Infof("g_lastidfile_path:%+v", g_lastidfile_path)
	my.Logger.Infof("sync starttime:%+v, sync endtime:%+v", my.start_time, my.end_time)
	my.Logger.Infof("g_sync_part_time:%+v", g_sync_part_time)
	my.Logger.Infof("NewBinLogMgr ok")

	return my
}

func (mgr *BinLogMgr) GetSyncPartTime() bool {
	return g_sync_part_time
}

func (mgr *BinLogMgr) GetLastOkIdFileMap() map[string]int {
	return g_LastOkIdCache
}

func (mgr *BinLogMgr) Read() *protocal.MsgMysqlBody {
	info, isClose := <-mgr.EventChan
	if !isClose {
		mgr.Logger.Infof("channel closed!")
		return nil
	}
	return info
}

func (mgr *BinLogMgr) Write(info *protocal.MsgMysqlBody) int {
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
