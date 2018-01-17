package main

import (
	"./binlogmgr"
	"./fdfsmgr"
	"./logger"
	"./mysqlmgr"
	"./cluster_sync"
	"./tair"
	"./transfer"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	//	"reflect"
	//	"time"
)

//var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

type Config struct {
	LogPath       string   `json:"log_path"`                  //各级别日志路径
	MysqlDsn      string   `json:"mysql_dsn"`                 //用户结果表msql
	MysqlDsnTask  string   `json:"mysql_dsn_task"`            //失败的任务表mql
	ListenPort    int      `json:"listen_port"`               //监听端口号
	TrackerServer []string `json:"tracker_server"`            //存储服务器
	MinConnection int      `json:"fdfs_min_connection_count"` //最小连接个数
	MaxConnection int      `json:"fdfs_max_connection_count"` // 最大连接个数
	TairClient    string   `json:"tair_client"`               // tairclient 地址
	TairServer    []string `json:"tair_server"`               // tair configserver 地址
	LastIdfile    string   `json:"lastidfile"`                // 接收id记录的文件路径，用于记录最后成功接收的id，为回复数据迁移准备
	Interval      int      `json:"interval"`                  // 定时时间间隔，单位秒
	SyncStartTime string   `json:"sync_start_time"`           // 开始同步时间
	SyncEndTime   string   `json:"sync_end_time"`             // 结束同步时间
	UploadServer  string   `json:"upload_server"`             // 负责同步的上传机接收请求地址
	FdfsBackupIp  string   `json:"fdfs_standby_ip"`           // 负责数据迁移服务的地址
	PipeLen       int      `json:"pipe_len"`                  // 接收上传机消息缓存管道大小，
	IsOpen        int      `json:"open_sync"`                 // 是否开启上传机数据迁移0表示开启，1表示开启不依赖上传机的数据迁移
	IsRestart     int      `json:"restart_flag"`              // 不依赖上传机的数据迁移生效即IsOpen=1时，获取所有大于收到id的数据库结果表，开始同步数据
	EsServer      []string `json:"es_server"`                 // 统计es的地址
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

func signalHandle(l *log.Logger) {
	l.Infof("signalHandle start.")
	for {
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGHUP, syscall.SIGUSR2)
		sig := <-ch
		l.Infof("Signal received: %v", sig)
		switch sig {
		case syscall.SIGHUP:
			l.Infof("init\n")
			//			os.Exit(1)
		case syscall.SIGUSR2:
			l.Infof("user2\n")
		default:
			l.Infof("get sig=%v\n", sig)
		}
	}
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

	//go signalHandle(l) //用go程执行信号量处理函数

	c := logger.GetLogger(cfg.LogPath, "tair")
	d := logger.GetLogger(cfg.LogPath, "binlog")
	f := logger.GetLogger(cfg.LogPath, "fdfs")
	g := logger.GetLogger(cfg.LogPath, "transmgr")
	r := logger.GetLogger(cfg.LogPath, "mysqlmgr")
	s := logger.GetLogger(cfg.LogPath, "sync")

	pMysqlMgr := mysqlmgr.NewMysqlMgr(cfg.MysqlDsn, cfg.MysqlDsnTask, cfg.FdfsBackupIp, r)
	if pMysqlMgr == nil {
		l.Errorf("NewMysqlMgr fail")
		return
	}

	pBinlog := binlogmgr.NewBinLogMgr(cfg.LastIdfile, pMysqlMgr, cfg.SyncStartTime,
		cfg.SyncEndTime, cfg.PipeLen, d)
	if pBinlog == nil {
		l.Errorf("NewBinLogMgr fail")
		return
	}

	pFdfsmgr := fdfsmgr.NewClient(cfg.TrackerServer, f, cfg.MinConnection, cfg.MaxConnection)
	if pFdfsmgr == nil {
		l.Errorf("NewClient fail")
		return
	}

	pTair := tair.NewTairClient(cfg.TairServer, cfg.TairClient, cfg.FdfsBackupIp, c)
	if pTair == nil {
		l.Errorf("NewTairClient fail")
		return
	}

	pTransferMgr := transfer.NewTransferMgr(pFdfsmgr, pTair, cfg.FdfsBackupIp, g)
	if pTransferMgr == nil {
		l.Errorf("TransferMgr fail")
		return
	}

//	pEsMgr := esmgr.NewEsMgr(cfg.EsServer, e)
//	if pEsMgr == nil {
//		l.Errorf("NewEsMgr fail")
//		return
//	}

	pSync := cluster_sync.NewSyncMgr(cfg.Interval, cfg.UploadServer, pBinlog, pTransferMgr, pMysqlMgr, nil, s)
	if pSync == nil {
		l.Errorf("NewSyncMgr fail")
		return
	}

	pSync.SetFlag(cfg.IsOpen, cfg.IsRestart)
	go pSync.RunDateMigrate()

	m := martini.Classic()
	
	m.Post("/uploadput", pSync.UploadPut)
	m.Post("/fdfsput", pSync.FastdfsPutData)
	m.Post("/fdfsget", pSync.FastdfsGetData)
	m.Post("/fdfsdelete", pSync.FastdfsDeleteData)
	m.Post("/mysqlreceive", pSync.MysqlReceive)
	m.Post("/tairreceive", pSync.TairReceive)

	m.Run()
}
