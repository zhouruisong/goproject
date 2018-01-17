package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"bytes"
	"net/http"
	"io/ioutil"
	"time"
	"os"
	"./protocal"
	"./logger"
	"github.com/go-martini/martini"
	log "github.com/Sirupsen/logrus"
)

var (
	f *log.Logger = nil
	fileBuffer []byte
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
	Dbconns         int      `json:"dbconns"`
	Dbidle          int      `json:"dbidle"`
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

func tairget(prefix, key string) {	
//	time.Sleep(10 * time.Second)
//	
	keys := protocal.SendTairGet {
		Prefix: prefix,
		Key: key,
	}
	
	var msg protocal.SednTairGetBody
	msg.Keys = append(msg.Keys, keys)
	
//	f.Infof("msg: %+v", msg)
	buf, err := json.Marshal(msg)
	if err != nil {
		f.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return
	}
	
	url := fmt.Sprintf("http://%v/getfromtair", "192.168.110.30:3000")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", "192.168.110.30")

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		f.Errorf("http post return failed.err: %v , buf: %+v", err, buf)
		return
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		f.Errorf("ioutil readall failed.err:%v", err)
		return
	}

	var ret protocal.RetTairGet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		f.Errorf("cannot decode req body Error, err:%v", err)
		return
	}
	    
	f.Infof("tairget ret: %+v", ret)
	return
}

func tairput(id string) {	
//	time.Sleep(10 * time.Second)
	
	keys := protocal.SendTairPut {
		Prefix    : "xylive-a-upload.kascend.com",
		Key       : "/chushou_rookie/89cf951384e54ed6bcdfbd2afc0f1e1e/test.m3u8",
		Value     : id,
		CreateTime: 1484591670,
		ExpireTime: 7776000,
	}
	
	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)
	
//	f.Infof("msg: %+v", msg)
	buf, err := json.Marshal(msg)
	if err != nil {
		f.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return
	}
	
	url := fmt.Sprintf("http://%v/puttotair", "192.168.110.30:3000")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", "192.168.110.30")

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		f.Errorf("http post return failed.err: %v , buf: %+v", err, buf)
		return
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		f.Errorf("ioutil readall failed.err:%v", err)
		return
	}

	var ret protocal.RetTairPut
	err = json.Unmarshal(result, &ret)
	if err != nil {
		f.Errorf("cannot decode req body Error, err:%v", err)
		return
	}
	    
	f.Infof("tairput ret: %+v", ret)
	
	tairget(keys.Prefix, keys.Key)
	
	return
}

func downloadbuff(id string, i int) {
//	time.Sleep(10 * time.Second)
	f.Infof("downloadbuff id: %+v, i: %+v", id, i)
	msg := protocal.CentreDownloadFile {
		Id: id,
	}
	
//	f.Infof("msg: %+v", msg)
	buf, err := json.Marshal(msg)
	if err != nil {
		f.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return
	}
	
	url := fmt.Sprintf("http://%v/getfastdfs", "192.168.110.30:3000")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", "192.168.110.30")

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		f.Errorf("http post return failed.err: %v , buf: %+v", err, buf)
		return
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		f.Errorf("ioutil readall failed.err:%v", err)
		return
	}

	var ret protocal.RetCentreDownloadFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		f.Errorf("cannot decode req body Error, err:%v", err)
		return
	}
	
//	file, _ := os.Create("tmp.txt")  
//	defer file.Close()
//	file.Write(ret.Content)
    
	f.Infof("downloadbuff ret.Errno: %+v, i: %+v", ret.Errno, i)
	return
}

func uploadbuff(fileBuffer []byte, fileSize int64, num int) {	
	time.Sleep(10 * time.Second)
	
	for i := 0; i < 1; i++ {

	f.Infof("uploadbuff fileSize: %+v, num: %+v", fileSize, num)
	
	msg := protocal.CentreUploadFile {
		Filename: "testfile",
		Content: fileBuffer,
	}
	
//	f.Infof("msg: %s", msg)
	buf, err := json.Marshal(msg)
	if err != nil {
		f.Errorf("Marshal failed.err:%v, Filename: %+v", err, msg.Filename)
		return
	}
	
//	f.Infof("after Marshal")
	url := fmt.Sprintf("http://%v/putfastdfs", "192.168.110.30:3000")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", "192.168.110.30")

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		f.Errorf("http post return failed.err: %v , Filename: %+v", err, msg.Filename)
		return
	}
	
//	f.Infof("after Post")
	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		f.Errorf("ioutil readall failed.err:%v", err)
		return
	}

//	f.Infof("after ReadAll",)
	var ret protocal.RetCentreUploadFile
	err = json.Unmarshal(result, &ret)
	if err != nil {
		f.Errorf("cannot decode req body Error, err:%v", err)
		return
	}
	
	f.Infof("uploadbuff ret: %+v, i: %+v", ret.Id, num)
	
	downloadbuff(ret.Id, num)
	tairput(ret.Id)
	}

	return
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

	f = logger.GetLogger(cfg.LogPath, "test")

	//file, err := os.Open("/code/myselfgo/src/myproject/cluster_backup/bin/testfile") // For read access.
	
	file, err := os.Open("/code/rpm/tfdfs-1.1.1-1.el6.x86_64.rpm") // For read access.
	defer file.Close()
	if err != nil {
		f.Fatal(err)
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	
	fileBuffer = make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		f.Fatal(err)
	}

	for i := 0; i < 1; i++ {
//		f.Infof("thread %+v run", i)
		go uploadbuff(fileBuffer, fileSize, i)
//		go tairget("xylive-a-upload.kascend.com", 
//			"/chushou_rookie/89cf951384e54ed6bcdfbd2afc0f1e1e/test.m3u8")
	}
	
	m := martini.Classic()
	port := fmt.Sprintf(":%d", cfg.ListenPort)
	l.Infof("listern %+v", port)
	f.Infof("listern %+v", port)
	m.RunOnAddr(port)//改变监听的端口
}
