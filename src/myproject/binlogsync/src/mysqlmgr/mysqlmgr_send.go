package mysqlmgr

import (
	"../protocal"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func (mgr *MysqlMgr) SendMysqlbuff(data *protocal.DbInfo, tablename string) int {
	// backup data need set to 1
	data.IsBackup = 1

	msg := protocal.MsgMysqlBody{
		TableName: tablename,
		Data:      *data,
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		mgr.Logger.Errorf("Marshal failed.err:%v, TableName: %+v", err, msg.TableName)
		return -1
	}

	url := fmt.Sprintf("http://%v/mysqlreceive", mgr.FdfsBackup)
	ip := strings.Split(mgr.FdfsBackup, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buf))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		mgr.Logger.Errorf("http post return failed.err: %v , Filename: %+v", err, msg.TableName)
		return -1
	}

	result, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		mgr.Logger.Errorf("ioutil readall failed.err:%v", err)
		return -1
	}

	var ret protocal.MsgMysqlRet
	err = json.Unmarshal(result, &ret)
	if err != nil {
		mgr.Logger.Errorf("cannot decode req body Error, err:%v", err)
		return -1
	}

	return ret.Errno
}
