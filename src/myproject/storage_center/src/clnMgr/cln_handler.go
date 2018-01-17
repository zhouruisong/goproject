package clnMgr

import (
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"io/ioutil"
	"net/http"
)

type RetInfo struct {
	Errno  int    `json:"errno"`
	Errmsg string `json:"errmsg"`
}

func (c *ClnMgr) StartCleaner(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")
	c.Run()
	res.Write([]byte(name)) // HTTP 200
}

func (c *ClnMgr) StopCleaner(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")
	c.Stop()
	res.Write([]byte(name)) // HTTP 200
}

func (c *ClnMgr) RestartCleaner(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")
	c.Restart()
	res.Write([]byte(name)) // HTTP 200
}

func (c *ClnMgr) DeleteStreamFile(res http.ResponseWriter, req *http.Request) {
	logid := fmt.Sprintf("%s", uuid.NewV4())
	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
	}
	r := RetInfo{
		Errno:  0,
		Errmsg: "",
	}
	b, err := json.Marshal(&r)
	if err != nil {

	}
	res.Write(b) // HTTP 200

	c.CleanStreamFile(logid, buf)
	//go c.CleanStreamFile(logid, buf)
}

func (c *ClnMgr) DeleteFile(res http.ResponseWriter, req *http.Request) {
	logid := fmt.Sprintf("%s", uuid.NewV4())
	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
	}
	r := RetInfo{
		Errno:  0,
		Errmsg: "",
	}
	b, err := json.Marshal(&r)
	if err != nil {

	}
	res.Write(b) // HTTP 200

	go c.CleanFile(logid, buf)
}

type RetMsg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *ClnScreenMgr) DeleteSrceenFileByEntry(res http.ResponseWriter, req *http.Request) {
	logid := fmt.Sprintf("%s", uuid.NewV4())
	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
	}

	r := RetMsg{
		Code:    0,
		Message: "",
	}

	b, err := json.Marshal(&r)
	if err != nil {
	}

	res.Write(b) // HTTP 200

	go c.CleanFile(1, logid, buf)
}

func (c *ClnScreenMgr) DeleteSrceenFileByDate(res http.ResponseWriter, req *http.Request) {
	logid := fmt.Sprintf("%s", uuid.NewV4())
	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
	}

	r := RetMsg{
		Code:    0,
		Message: "",
	}

	b, err := json.Marshal(&r)
	if err != nil {
	}

	res.Write(b) // HTTP 200
	go c.CleanFile(2, logid, buf)
}
