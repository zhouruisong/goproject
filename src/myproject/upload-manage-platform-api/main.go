package main

import (
	"./conf"
	"./static"
	"bytes"
	"encoding/base64"
	"fmt"
	"log"
	//"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func GetUploadServerDispatch(w http.ResponseWriter, r *http.Request) {
	ret := conf.GetUploadServerDispatch()
	// fmt.Println(ret)
	fmt.Fprintf(w, ret) //这个写入到w的是输出到客户端的
}

func SetUploadServerDispatch(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	// fmt.Println(r)
	param_act, found1 := r.Form["act"]
	param_dispatchstatus, found2 := r.Form["dispatchstatus"]
	param_ip, found3 := r.Form["ip"]

	if !(found1 && found2 && found3) {
		fmt.Fprint(w, "请勿非法访问")
		return
	}
	act, err := strconv.Atoi(param_act[0])
	if err != nil {
		fmt.Fprint(w, "请求非法")
		return
	}
	dispatchstatus, err := strconv.Atoi(param_dispatchstatus[0])
	if err != nil {
		fmt.Fprint(w, "请求非法")
		return
	}

	ret := conf.SetUploadServerDispatch(param_ip[0], act, dispatchstatus)
	// fmt.Println(ret)
	fmt.Fprintf(w, ret) //这个写入到w的是输出到客户端的
}

func BasicAuth(w http.ResponseWriter, r *http.Request) {
	// fmt.Println(r.Header)
	auth := r.Header.Get("Authorization")
	if len(auth) == 0 {
		// 认证失败，提示 401 Unauthorized
		// Restricted 可以改成其他的值
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		// 401 状态码
		w.WriteHeader(http.StatusUnauthorized)

		result := "{"
		result = result + "\"result\":" + "\"ture\"" + ","
		result = result + "\"code\":" + "\"" + strconv.Itoa(401) + "\"" + "}"
		fmt.Fprintf(w, result) //这个写入到w的是输出到客户端的
		return
	}

	r.ParseForm()
	basicAuthPrefix := "Basic "

	// 如果是 http basic auth
	if strings.HasPrefix(auth, basicAuthPrefix) {
		// 解码认证信息
		payload, err := base64.StdEncoding.DecodeString(auth[len(basicAuthPrefix):])
		if err == nil {
			pair := bytes.SplitN(payload, []byte(":"), 2)
			user := string(pair[0])
			pwd := string(pair[1])
			ret := conf.LoginCheck(user, pwd)
			if ret != 0 {
				// 认证失败，提示 401 Unauthorized
				// Restricted 可以改成其他的值
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				// 401 状态码
				w.WriteHeader(http.StatusUnauthorized)
			} else {
				// fmt.Println("Welcome to the home page!")
				path := r.URL.Path[1:]
				// fmt.Println(path)
				// chrome send favicon.ico
				if path == "favicon.ico" {
					http.NotFound(w, r)
					return
				}
				if path == "" {
					path = "index.html"
				}

				pwd, _ := os.Getwd()
				path = pwd + "/" + path
				// 提供文件 第一次为index.html，以后依次为index.html中定义加载的js和css
				http.ServeFile(w, r, path)
			}
		}
	} else {
		fmt.Println("not http basic auth!")
		fmt.Fprint(w, "not http basic auth")
	}
}

func OperateUploadServer(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	param_op, found1 := r.Form["op"]
	param_ip, _ := r.Form["ip"]
	param_node, _ := r.Form["nodenumber"]
	param_explain, _ := r.Form["explain"]
	param_act, _ := r.Form["act"]
	param_dispatchstatus, _ := r.Form["dispatchstatus"]
	param_nodekey, _ := r.Form["nodenumberkey"]

	op, err := strconv.Atoi(param_op[0])
	if err != nil {
		fmt.Fprint(w, "请求非法")
		return
	}

	if !(found1) {
		fmt.Fprint(w, "op is null")
		return
	}

	para := new(conf.InputServerPara)
	para.Op = op

	if op == 1 {
		nodenum, err := strconv.Atoi(param_node[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		para.Nodenumber = nodenum
	}

	if op == 0 || op == 2 {
		nodenum, err := strconv.Atoi(param_node[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		if op == 2 {
			nodenumkey, err := strconv.Atoi(param_nodekey[0])
			if err != nil {
				fmt.Fprint(w, "请求非法")
				return
			}
			para.Nodenumberkey = nodenumkey
		}

		act, err := strconv.Atoi(param_act[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		dispatchstatus, err := strconv.Atoi(param_dispatchstatus[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		para.Ip = param_ip[0]
		para.Nodenumber = nodenum
		para.Act = act
		para.Dispatchstatus = dispatchstatus
		para.Explain = param_explain[0]
		// fmt.Println(para)
	}

	if op == 3 {
		nodenum, err := strconv.Atoi(param_node[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		para.Nodenumber = nodenum
	}

	ret := conf.OperateUploadServer(para)
	// fmt.Println(ret)
	fmt.Fprintf(w, ret) //这个写入到w的是输出到客户端的
}

func OperateUploadNode(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	// fmt.Println(r)
	param_op, found1 := r.Form["op"]
	param_node, found2 := r.Form["nodenumber"]
	param_nanme, _ := r.Form["nodename"]
	param_cdnnodeid, _ := r.Form["cdnnodeid"]
	// param_hdfsname, _ := r.Form["hdfsname"]

	if !(found1 && found2) {
		fmt.Fprint(w, "请勿非法访问")
		return
	}
	op, err := strconv.Atoi(param_op[0])
	if err != nil {
		fmt.Fprint(w, "请求非法")
		return
	}
	nodenum, err := strconv.Atoi(param_node[0])
	if err != nil {
		fmt.Fprint(w, "请求非法")
		return
	}

	paranode := new(conf.InputNodePara)
	paranode.Op = op
	paranode.Nodenumber = nodenum

	if op == 0 || op == 2 {
		param_ip, _ := r.Form["ip"]
		param_explain, _ := r.Form["explain"]
		// param_act, _ := r.Form["act"]
		// param_dispatchstatus, _ := r.Form["dispatchstatus"]
		param_nodekey, _ := r.Form["nodenumberkey"]
		param_type, _ := r.Form["type"]

		nodenum, err := strconv.Atoi(param_node[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		paranode.Type = 2

		if op == 2 {
			nodenumkey, err := strconv.Atoi(param_nodekey[0])
			if err != nil {
				fmt.Fprint(w, "请求非法")
				return
			}
			paranode.Nodenumberkey = nodenumkey

			type_new, err := strconv.Atoi(param_type[0])
			if err != nil {
				fmt.Fprint(w, "请求非法")
				return
			}
			paranode.Type = type_new
		}

		cdnnodeid, err := strconv.Atoi(param_cdnnodeid[0])
		if err != nil {
			fmt.Fprint(w, "请求非法")
			return
		}

		paranode.Ip = param_ip[0]
		paranode.Nodename = param_nanme[0]
		paranode.Nodenumber = nodenum
		paranode.Cdnnodeid = cdnnodeid
		// paranode.Hdfsname = param_hdfsname[0]
		// paranode.Act = act
		// paranode.Dispatchstatus = dispatchstatus
		paranode.Explain = param_explain[0]
	}

	ret := conf.OperateUploadNode(paranode)
	// fmt.Println(ret)
	fmt.Fprintf(w, ret) //这个写入到w的是输出到客户端的
}

func main() {
	// new mux
	mux := http.NewServeMux()
	// reject function
	//mux.HandleFunc("/", BasicAuth)
	mux.HandleFunc("/getUploadStatus", GetUploadServerDispatch) //请求绑定函数
	mux.HandleFunc("/setUploadStatus", SetUploadServerDispatch)
	mux.HandleFunc("/opUploadServer", OperateUploadServer)
	mux.HandleFunc("/opUploadNode", OperateUploadNode)
	mux.HandleFunc("/volumeData", static.Handler)

	//addrs, err := net.InterfaceAddrs()
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(1)
	//}

	var ip = "192.168.110.30"
	//	for _, address := range addrs {
	//		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
	//			if ipnet.IP.To4() != nil {
	//				//fmt.Println(ipnet.IP.String())
	//				ip = ipnet.IP.String()
	//				break
	//			}
	//		}
	//	}

	var url = ip + ":" + "8080"
	err := http.ListenAndServe(url, mux) //设置监听的端口
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
