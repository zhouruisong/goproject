package ruleMgr

import (
	"net/http"
)

func (r *RuleMgr) AddCleanRule(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")

	res.Write([]byte(name)) // HTTP 200
}

func (r *RuleMgr) DelCleanRule(res http.ResponseWriter, req *http.Request) {
	name := req.PostFormValue("name")

	res.Write([]byte(name)) // HTTP 200
}

func (r *RuleMgr) GetAllCleanRules(res http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	name := query["name"][0]

	res.Write([]byte(name)) // HTTP 200
}
