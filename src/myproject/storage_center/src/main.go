package main

import (
	"./clnMgr"
	"./devMgr"
	"./logger"
	"./ruleMgr"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
)

type Config struct {
	LogPath    string `json:"log_path"`    //各级别日志路径
	MysqlDsn   string `json:"mysql_dsn"`   //后台存储dsn
	ListenPort int    `json:"listen_port"` //监听端口号
	Interval   int    `json:"interval"`    //时间间隔
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

	l := logger.GetLogger(cfg.LogPath, "storage")
	l.Infof("start service.")

	d := logger.GetLogger(cfg.LogPath, "devmgr")
	r := logger.GetLogger(cfg.LogPath, "rulmgr")
	c := logger.GetLogger(cfg.LogPath, "clnmgr")

	m := martini.Classic()
	d_mgr := devMgr.NewDevMgr(cfg.MysqlDsn, d)
	r_mgr := ruleMgr.NewRuleMgr(cfg.MysqlDsn, r)
	
	if cfg.Interval == 0 {
		cfg.Interval = 2
	}
	c_mgr := clnMgr.NewClnMgr(d_mgr, r_mgr, c, cfg.Interval)

	dscreen := logger.GetLogger(cfg.LogPath, "dev_screenmgr")
	s := logger.GetLogger(cfg.LogPath, "screenmgr")

	dsrc_mgr := devMgr.NewScreenDevMgr(cfg.MysqlDsn, dscreen)
	screen_mgr := clnMgr.NewSrceenClnMgr(dsrc_mgr, s)

	m.Post("/dev/add", d_mgr.AddDevice)
	m.Post("/dev/del", d_mgr.DelDevice)
	m.Get("/dev/get", d_mgr.GetAllDevices)
	m.Get("/dev/group/get", d_mgr.GetDevGroupInfo)

	m.Post("/cluster/add", d_mgr.AddCluster)
	m.Post("/cluster/del", d_mgr.DelCluster)
	m.Get("/cluster/get", d_mgr.GetAllClusters)

	m.Post("/rule/add", r_mgr.AddCleanRule)
	m.Post("/rule/del", r_mgr.DelCleanRule)
	m.Get("/rule/get", r_mgr.GetAllCleanRules)

	m.Post("/clean/start", c_mgr.StartCleaner)
	m.Post("/clean/stop", c_mgr.StopCleaner)
	m.Post("/clean/restart", c_mgr.RestartCleaner)
	
	m.Post("/clean/delfile", c_mgr.DeleteFile)

	m.Post("/clean/delstreamfile", c_mgr.DeleteStreamFile)
	m.Post("/clean/delfilebyentry", screen_mgr.DeleteSrceenFileByEntry)
	m.Post("/clean/delfilebydate", screen_mgr.DeleteSrceenFileByDate)
	//============test==============
	m.Post("/test", d_mgr.HubForTest)

	// go c_mgr.Run()
	m.Run()
}
