package main

import (
	"./binlogmgr"
	"./esmgr"
	"./fdfsmgr"
	"./logger"
	"./mysqlmgr"
	"./sync"
	"./tair"
	"./transfer"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
	//	"reflect"
	//	"time"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

type Config struct {
	LogPath       string   `json:"log_path"`  //各级别日志路径
	MysqlDsn      string   `json:"mysql_dsn"` //后台存储dsn
	MysqlIp       string   `json:"mysql_ip"`
	MysqlUserName string   `json:"mysql_username"`
	MysqlPassword string   `json:"mysql_password"`
	MsqlPort      uint16   `json:"mysql_port"`
	ListenPort    int      `json:"listen_port"` //监听端口号
	TrackerServer []string `json:"tracker_server"`
	MinConnection int      `json:"fdfs_min_connection_count"`
	MaxConnection int      `json:"fdfs_max_connection_count"`
	TairClient    string   `json:"tair_client"`
	TairServer    []string `json:"tair_server"`
	ServerId      uint32   `json:"server_Id"`
	EachSyncNum   int      `json:"each_sync_num"`
	Dbconns       int      `json:"dbconns"`
	Dbidle        int      `json:"dbidle"`
	Binlogfile    string   `json:"binlogfile"`
	LastIdfile    string   `json:"lastidfile"`
	SyncStartTime string   `json:"sync_start_time"`
	SyncEndTime   string   `json:"sync_end_time"`
	UploadServer  string   `json:"upload_server"`
	FdfsBackupIp  string   `json:"fdfs_standby_ip"`
	IsOpen        int      `json:"open_sync"`
	EsServer      []string `json:"es_server"`
}

func loadConfig(path string) *Config {
	if len(path) == 0 {
		panic("path of conifg is null.")
	}

	_, err := os.Stat(path)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	var cfg Config
	b, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b, &cfg)
	if err != nil {
		panic(err)
	}

	return &cfg
}

func main() {
	var cfg_path string
	flag.StringVar(&cfg_path, "conf", "../conf/conf.json", "config file path")
	flag.Parse()
	fmt.Println(cfg_path)

	cfg := loadConfig(cfg_path)

	l := logger.GetLogger(cfg.LogPath, "init")
	l.Infof("cluster backup start.")

	l.Infof("cluster backup start.%+v", cfg)

	c := logger.GetLogger(cfg.LogPath, "tair")
	d := logger.GetLogger(cfg.LogPath, "binlog")
	e := logger.GetLogger(cfg.LogPath, "es")
	f := logger.GetLogger(cfg.LogPath, "fdfs")
	g := logger.GetLogger(cfg.LogPath, "clustermgr")

	r := logger.GetLogger(cfg.LogPath, "mysqlmgr")
	s := logger.GetLogger(cfg.LogPath, "sync")

	pBinlog := binlogmgr.NewBinLogMgr(cfg.MysqlIp, cfg.MsqlPort,
		cfg.MysqlUserName, cfg.MysqlPassword, cfg.ServerId, cfg.EachSyncNum,
		cfg.Dbconns, cfg.Dbidle, cfg.Binlogfile, cfg.LastIdfile, cfg.SyncStartTime,
		cfg.SyncEndTime, d)
	if pBinlog == nil {
		l.Errorf("NewBinLogMgr fail")
		return
	}

	pFdfsmgr := fdfsmgr.NewClient(cfg.TrackerServer, f, cfg.MinConnection, cfg.MaxConnection)
	if pFdfsmgr == nil {
		l.Errorf("NewClient fail")
		return
	}

	pTair := tair.NewTairClient(cfg.TairServer, cfg.TairClient, c)
	if pTair == nil {
		l.Errorf("NewTairClient fail")
		return
	}

	pTransferMgr := transfer.NewTransferMgr(pFdfsmgr, pTair, cfg.FdfsBackupIp, g)
	if pTransferMgr == nil {
		l.Errorf("TransferMgr fail")
		return
	}

	pMysqlMgr := mysqlmgr.NewMysqlMgr(cfg.MysqlDsn, cfg.Dbconns, cfg.Dbidle, cfg.FdfsBackupIp, r)
	if pMysqlMgr == nil {
		l.Errorf("NewMysqlMgr fail")
		return
	}

	pEsMgr := esmgr.NewEsMgr(cfg.EsServer, e)
	if pEsMgr == nil {
		l.Errorf("NewEsMgr fail")
		return
	}

	pSync := sync.NewSyncMgr(cfg.UploadServer, pBinlog, pTransferMgr, pMysqlMgr, pEsMgr, s)
	if pSync == nil {
		l.Errorf("NewSyncMgr fail")
		return
	}

	pSync.SetFlag(cfg.IsOpen)
	go pSync.IncreaseSync()

	m := martini.Classic()

	m.Post("/fdfsput", pTransferMgr.FastdfsPutData)
	m.Post("/fdfsget", pTransferMgr.FastdfsGetData)
	m.Post("/fdfsdelete", pTransferMgr.FastdfsDeleteData)

	m.Post("/mysqlreceive", pMysqlMgr.MysqlReceive)

	m.RunOnAddr("192.168.110.30:3005")
}
