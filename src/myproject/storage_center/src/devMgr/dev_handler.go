package devMgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type DevGroupInfoRet struct {
	Errno  int         `json:"errno"`
	Errmsg string      `json:"errmsg"`
	Groups []GroupInfo `json:"groups"`
}

type GroupInfo struct {
	Groupid string   `json:"group"`
	Hosts   []string `json:"hosts"`
}

type DevInfosRet struct {
	Errno  int      `json:"errno"`
	Errmsg string   `json:"errmsg"`
	Group  string   `json:"group"`
	Hosts  []string `json:"hosts"`
}

func (d *DevMgr) HubForTest(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")
	result, _ := ioutil.ReadAll(req.Body)
	req.Body.Close()
	fmt.Printf("have result:%s\n", result)

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) AddDevice(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) GetAllDevices(res http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	name := query["name"][0]

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) DelDevice(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) AddCluster(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) DelCluster(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) GetAllClusters(res http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	name := query["name"][0]

	res.Write([]byte(name)) // HTTP 200
}

func (d *DevMgr) GetDevGroupInfo(res http.ResponseWriter, req *http.Request) {
	ret := DevGroupInfoRet{
		Errno:  0,
		Errmsg: "",
	}

	var host string
	var r int
	var info []DevInfo
	var exsit bool = false
	ids := make([]string, 0, 8)
	grprets := make([]GroupInfo, 0)

	query := req.URL.Query()
	if 0 == len(query["host"]) {
		ret.Errno = -1
		ret.Errmsg = "input host is null"
		goto END
	}

	host = query["host"][0]
	if len(host) == 0 {
		ret.Errno = -1
		ret.Errmsg = "input host is null"
		goto END
	}

	r, info = d.getDevGroupInfo(host)
	if r != 0 || len(info) == 0 {
		ret.Errno = -1
		ret.Errmsg = "query is null"
		goto END
	}

	//找出有多少个组,将所有的id都提取出来
	for i := 0; i < len(info); i++ {
		exsit = false
		for _, id := range ids {
			if id == info[i].Groupid {
				exsit = true
			}
		}
		if exsit == false {
			ids = append(ids, info[i].Groupid)
		}
	}
	//按照分组id取分组
	for _, id := range ids {
		grpinfo := GroupInfo{
			Groupid: id,
			Hosts:   make([]string, 0, 2),
		}
		for _, inf := range info {
			if inf.Groupid == id {
				grpinfo.Hosts = append(grpinfo.Hosts, inf.Ip)
			}
		}
		grprets = append(grprets, grpinfo)
	}

	ret.Groups = grprets

END:
	b, err := json.Marshal(ret)
	if err != nil {
		return
	}

	res.Write(b)
}
