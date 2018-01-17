package main

import (
	// "time"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"./domainmgr"
	"./fdfs_client"
	"./logger"
	"./sync"
	"./tair"
	"github.com/go-martini/martini"
)

type Config struct {
	LogPath         string   `json:"log_path"`  //各级别日志路径
	MysqlDsn        string   `json:"mysql_dsn"` //后台存储dsn
	MysqlIp         string   `json:"mysql_ip"`
	MysqlUserName   string   `json:"mysql_username"`
	MysqlPassword   string   `json:"mysql_password"`
	MsqlPort        int      `json:"mysql_port"`
	MysqlBackupList string   `json:"mysql_standby_list"`
	ListenPort      int      `json:"listen_port"` //监听端口号
	TrackerServer   []string `json:"tracker_server"`
	MinConnection   int      `json:"fdfs_min_connection_count"`
	MaxConnection   int      `json:"fdfs_max_connection_count"`
	TairClient      string   `json:"tair_client"`
	TairServer      []string `json:"tair_server"`
	ServerId        int      `json:"server_Id"`
	EachSyncNum     int      `json:"each_sync_num"`
	Dbconns         int       `json:"dbconns"`
	Dbidle          int       `json:"dbidle"`
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

	d := logger.GetLogger(cfg.LogPath, "binlog")
	r := logger.GetLogger(cfg.LogPath, "domainmgr")
	c := logger.GetLogger(cfg.LogPath, "tair")
	s := logger.GetLogger(cfg.LogPath, "sync")
	f := logger.GetLogger(cfg.LogPath, "fdfs")

	pBinlog := domainmgr.NewBinLogMgr(cfg.MysqlIp, cfg.MsqlPort, cfg.MysqlUserName,
		cfg.MysqlPassword, cfg.ServerId, cfg.EachSyncNum, cfg.Dbconns, cfg.Dbidle, d)
	if pBinlog == nil {
		l.Errorf("NewBinLogMgr fail")
		return
	}

	pStreaminfo := domainmgr.NewStreamMgr(cfg.MysqlDsn, cfg.MysqlBackupList, r)
	if pStreaminfo == nil {
		l.Errorf("NewStreamMgr fail")
		return
	}

	pTair := tair.NewTairClient(cfg.TairServer, cfg.TairClient, c)
	if pTair == nil {
		l.Errorf("NewTairClient fail")
		return
	}

	pClient, err := fdfs_client.NewFdfsClient(cfg.TrackerServer, f,
		cfg.MinConnection, cfg.MaxConnection)
	if err != nil {
		l.Errorf("NewFdfsClient fail")
		return
	}

	pSync := sync.NewSyncMgr(pStreaminfo, pBinlog, pTair, pClient, s)
	if pSync == nil {
		l.Errorf("NewSyncMgr fail")
		return
	}

	go pSync.IncreaseSync()
	//go pSync.TotalSync()

	m := martini.Classic()
	m.Post("/checkdata", pSync.RecviveCheckData)
	m.Post("/mysqlsync", pSync.ReceiveDbData)
	// m.Post("/receivebuff", pSync.ReceiveBuff)
	
	m.Run()

	// time.Sleep(time.Second * 2)

	// m.Post("/receivefile", sync.ReceiveData)
	// m.Post("/sendfile", sync.SendData)

	// respUpload, err := client.UploadAppenderByFilename("/usr/local/sandai/tfdfs/bin/start_tairclient.sh")
	// if err != nil {
	// 	l.Infof("UploadAppenderByFilename fail")
	// 	return
	// }

	// l.Infof("UploadAppenderByFilename ok, %s, %s", respUpload.GroupName, respUpload.RemoteFileId)

	// respDownload, err1 := client.DownloadToBuffer(respUpload.RemoteFileId, 0, 513)
	// if err1 != nil {
	// 	l.Infof("DownloadToBuffer fail")
	// 	return
	// }

	// l.Infof("DownloadToBuffer ok, %s, %v, %d", respDownload.RemoteFileId,
	// 	respDownload.Content, respDownload.DownloadSize)

	//go pSync.IncreaseSync()
	// return
	// m := martini.Classic()
	// m.Post("/tair/pput", d_mgr.AddDevice)
	// m.Post("/dev/pget", d_mgr.AddDevice)
}
