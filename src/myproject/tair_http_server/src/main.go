package main

import (
	"./logger"
	"./tair"
	"encoding/json"
	"net/http"
	//"net/http/pprof"
	//"runtime"
	"flag"
	"fmt"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
)

type Config struct {
	LogPath           string `json:"log_path"`           //各级别日志路径
	TairClient        string `json:"tair_client"`        //tairclient 启动ip地址
	TairServer_port   string `json:"tair_server_port"`   //tair configserver 备地址
	TairServer_master string `json:"tair_server_master"` //tair configserver 主地址
	TairServer_slave  string `json:"tair_server_slave"`  //tair configserver 备地址
	TairConsumer      string `json:"tair_consumer"`      //tair 消费进程数
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

func HandleTair(w http.ResponseWriter, r *http.Request) {
	tair.Tair(w, r)
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

	t := logger.GetLogger(cfg.LogPath, "tair")
	tair.TairLoadConf(cfg.TairServer_master, cfg.TairServer_slave, cfg.TairServer_port, cfg.TairConsumer, t)
	if err := tair.Init(); err != nil {
		l.Errorf("tair init fail,error:%+v", err)
		return
	}

	//for url, f := range tair.Urls {
	//	http.HandleFunc(url, f)
	//}

	m := martini.Classic()
	m.Post("/tair", HandleTair)
	m.RunOnAddr(cfg.TairClient)
	l.Infof("start on %v", cfg.TairClient)
}
