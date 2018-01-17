package binlogmgr

import (
	"../mysqlmgr"
	log "github.com/Sirupsen/logrus"
)

type BinLogMgr struct {
	Logger     *log.Logger
	pSql       *mysqlmgr.MysqlMgr
}

func NewBinLogMgr(psql *mysqlmgr.MysqlMgr, lg *log.Logger) *BinLogMgr {
	my := &BinLogMgr{
		Logger:    lg,
		pSql:      psql,
	}

	my.Logger.Infof("NewBinLogMgr ok")
	return my
}