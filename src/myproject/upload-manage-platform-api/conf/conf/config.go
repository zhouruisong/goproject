package conf

import (
	"bufio"
	"fmt"
	"io"
	"github.com/labix.org/v2/mgo"
	"github.com/labix.org/v2/mgo/bson"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// 操作枚举值
const (
	INSERT = iota
	DELETE
	UPDATE
	SELECT
	SELECTALL
)

type Config struct {
	Mymap map[string]string
	title string
}

type NodeInfo struct {
	Id_                    bson.ObjectId   `bson:"_id"`
	Id                     int
	Namenodeip             string
	Nodenumber             int
	Nodename               string
	Hdfsname               string
	HdfsURL                string
	Runstatus              int
	Status                 int
	Uploadstatus           int
	Encodestatus           int
	Cdnnodeid              int
	Ip                     string
	Cachesize              string
	Cacheuse               int
	Beattime               string
	Dispatchstatus         int
	Explain                string
	Act                    int
	DownloadLoadbalanceURL string
	Type                   int
}

type UploadServerAll struct {
  UploadServerInfos []UploadServerInfo
}

type UploadServerInfo struct {
	Id_            bson.ObjectId    `bson:"_id"`
	Act            int
	Beattime       string
	Cachesize      string
	Cacheuse       int
	Dispatchstatus int
	Explain        string
	Id             int
	Ip             string
	Nodenumber     int
	Runstatus      int
}

var configfile string
var mgoSession *mgo.Session
var mgoDb *mgo.Database
var myconfig *Config

func (c *Config) GetCurrPath() string {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	index := strings.LastIndex(path, string(os.PathSeparator))
	ret := path[:index]
	result := fmt.Sprintf("%s/%s", ret, configfile)
	// fmt.Println(result)
	return result
}

func (c *Config) InitConfig(path string) {
	c.Mymap = make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		s := strings.TrimSpace(string(b))
		//fmt.Println(s)
		if strings.Index(s, "#") == 0 {
			continue
		}

		n1 := strings.Index(s, "[")
		n2 := strings.LastIndex(s, "]")
		if n1 > -1 && n2 > -1 && n2 > n1+1 {
			c.title = strings.TrimSpace(s[n1+1 : n2])
			continue
		}

		if len(c.title) == 0 {
			continue
		}
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}

		frist := strings.TrimSpace(s[:index])
		if len(frist) == 0 {
			continue
		}
		second := strings.TrimSpace(s[index+1:])

		pos := strings.Index(second, "\t#")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " #")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, "\t//")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " //")
		if pos > -1 {
			second = second[0:pos]
		}

		if len(second) == 0 {
			continue
		}

		key := c.title + "_" + frist
		// key := frist
		// fmt.Println(key)
		// fmt.Println(frist)
		// fmt.Println(c.title)
		c.Mymap[key] = strings.TrimSpace(second)
		//fmt.Println(c.Mymap[key])
	}
}

func Read(node, key string) string {
	newkey := node + "_" + key
	// fmt.Println(key)
	//fmt.Println(node)
	if myconfig == nil {
		return ""	
	}
	// fmt.Println(myconfig.Mymap)
	v, found := myconfig.Mymap[newkey]
	if !found {
		return ""
	}
	return v
}


func ReadServerConf(confile string) string {
	configfile = confile
	if myconfig == nil {
		// fmt.Println("new myconfig")
		myconfig = new(Config)
		confpath := myconfig.GetCurrPath()
		myconfig.InitConfig(confpath)
	}

	ip := Read("server", "ip")
	port := Read("server", "port")

	url := fmt.Sprintf("%s:%s", ip, port)
	fmt.Println(url)
	return url
}

func Init(confile string) (*mgo.Session, *mgo.Database) {
	configfile = confile
	if myconfig == nil {
		// fmt.Println("new myconfig")
		myconfig = new(Config)
		confpath := myconfig.GetCurrPath()
		myconfig.InitConfig(confpath)
	}
	
	var username, password string
	if mgoSession == nil {
		ip := Read("upload", "ip")
		port := Read("upload", "port")
		username = Read("upload", "username")
		password = Read("upload", "password")

		URL := fmt.Sprintf("%s:%s", ip, port)

		var err error
		mgoSession, err = mgo.Dial(URL)
		if err != nil {
			panic(err)
		}
		// fmt.Println("new mgoSession")
	}

	if mgoDb == nil {
		mgoDb = mgoSession.DB("upload")
		error := mgoDb.Login(username, password)
		if error != nil {
			panic(error)
		}
		// fmt.Println("new mgoDb")
	}

	return mgoSession.Clone(), mgoDb
}
