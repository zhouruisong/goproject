package main

import (
	"./binlogmgr"
	"./cluster_sync"
	"./fdfsmgr"
	"./logger"
	"./mysqlmgr"
	"./tair"
	"./transfer"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
)

type Config struct {
	Role          int      `json:"role"`                      //0-client,1-server
	LogPath       string   `json:"log_path"`                  //各级别日志路径
	MysqlDsn      string   `json:"mysql_dsn"`                 //用户结果表msql
	ListenPort    int      `json:"listen_port"`               //监听端口号
	TrackerServer []string `json:"tracker_server"`            //存储服务器
	MinConnection int      `json:"fdfs_min_connection_count"` //最小连接个数
	MaxConnection int      `json:"fdfs_max_connection_count"` //最大连接个数
	TairClient    string   `json:"tair_client"`               //tairclient 地址
	TairServer    []string `json:"tair_server"`               //tair configserver 地址
	CusumeNum     int      `json:"cusume_num"`                //启动消费上传队列的协程数
	UpfileSrvip   string   `json:"upfile_srv_ip"`             //upfile server ip
	UpMachine     string   `json:"upmachine"`                 //上传机地址
	Cacheip       string   `json:"cacheip"`                   //cache地址，拼接存储下载地址
	CallBack      string   `json:"callback"`                  //上传成功回调给上层
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

	var m *martini.ClassicMartini

	//0-client,1-server
	if cfg.Role == 0 {
		c := logger.GetLogger(cfg.LogPath, "tair")
		d := logger.GetLogger(cfg.LogPath, "binlog")
		g := logger.GetLogger(cfg.LogPath, "transmgr")
		s := logger.GetLogger(cfg.LogPath, "sync")

		pBinlog := binlogmgr.NewBinLogMgr(nil, d)
		if pBinlog == nil {
			l.Errorf("NewBinLogMgr fail")
			return
		}

		pTair := tair.NewTairClient(cfg.TairServer, cfg.TairClient, cfg.UpfileSrvip, c)
		if pTair == nil {
			l.Errorf("NewTairClient fail")
			return
		}
		
		pTransferMgr := transfer.NewTransferMgr(nil,pTair,cfg.UpfileSrvip,cfg.UpMachine,cfg.CallBack,g)
		if pTransferMgr == nil {
			l.Errorf("TransferMgr fail")
			return
		}
		
		pSync := cluster_sync.NewSyncMgr(pBinlog,pTransferMgr,nil,cfg.CusumeNum,cfg.Cacheip,s)
		if pSync == nil {
			l.Errorf("NewSyncMgr fail")
			return
		}

		m = martini.Classic()
		//接收文件名和其他数据，将文件切片上传
		m.Post("/fileupload", pSync.FileUpload)

	} else if cfg.Role == 1 {
		c := logger.GetLogger(cfg.LogPath, "tair")
		d := logger.GetLogger(cfg.LogPath, "binlog")
		f := logger.GetLogger(cfg.LogPath, "fdfs")
		g := logger.GetLogger(cfg.LogPath, "transmgr")
		r := logger.GetLogger(cfg.LogPath, "mysqlmgr")
		s := logger.GetLogger(cfg.LogPath, "sync")

		pMysqlMgr := mysqlmgr.NewMysqlMgr(cfg.MysqlDsn, cfg.MysqlDsn, r)
		if pMysqlMgr == nil {
			l.Errorf("NewMysqlMgr fail")
			return
		}

		pBinlog := binlogmgr.NewBinLogMgr(pMysqlMgr, d)
		if pBinlog == nil {
			l.Errorf("NewBinLogMgr fail")
			return
		}

		pFdfsmgr := fdfsmgr.NewFdfsMgr(cfg.TrackerServer,f,cfg.MinConnection,cfg.MaxConnection)
		if pFdfsmgr == nil {
			l.Errorf("NewClient fail")
			return
		}

		pTair := tair.NewTairClient(cfg.TairServer, cfg.TairClient, "", c)
		if pTair == nil {
			l.Errorf("NewTairClient fail")
			return
		}

		pTransferMgr := transfer.NewTransferMgr(pFdfsmgr, pTair, "", "", "",g)
		if pTransferMgr == nil {
			l.Errorf("TransferMgr fail")
			return
		}

		pSync := cluster_sync.NewSyncMgr(pBinlog,pTransferMgr,pMysqlMgr,cfg.CusumeNum,"",s)
		if pSync == nil {
			l.Errorf("NewSyncMgr fail")
			return
		}

		m = martini.Classic()
		m.Post("/fdfsput", pSync.FastdfsPutData)
		m.Post("/fdfsget", pSync.FastdfsGetData)
		m.Post("/fdfsdelete", pSync.FastdfsDeleteData)
		m.Post("/tairreceive", pSync.TairReceive)
	}

	m.Run()
}
