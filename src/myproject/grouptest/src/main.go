package main

import (
	"./fdfsmgr"
	"./logger"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
)

//var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

type Config struct {
	LogPath       string   `json:"log_path"`                  //各级别日志路径
	ListenPort    int      `json:"listen_port"`               //监听端口号
	TrackerServer []string `json:"tracker_server"`            //存储服务器
	MinConnection int      `json:"fdfs_min_connection_count"` //最小连接个数
	MaxConnection int      `json:"fdfs_max_connection_count"` //最大连接个数
	Cache_ip_list []string `json:"cache_ip_list"`             //cache ip
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

	f := logger.GetLogger(cfg.LogPath, "fdfs")

	pFdfsmgr := fdfsmgr.NewClient(cfg.TrackerServer, cfg.Cache_ip_list, f, 
		cfg.MinConnection, cfg.MaxConnection)
	if pFdfsmgr == nil {
		l.Errorf("NewClient fail")
		return
	}

	m := martini.Classic()
	m.Post("/uploadbuff", pFdfsmgr.UploadFileToGroup)
	m.Run()
}
