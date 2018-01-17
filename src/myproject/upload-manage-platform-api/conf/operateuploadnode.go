// 上传机节点的CRUD接口

package conf

import (
	"./conf"
	"fmt"
	"strconv"
	"strings"
	"github.com/labix.org/v2/mgo"
	"github.com/labix.org/v2/mgo/bson"
)

type InputNodePara struct {
	Op             int
	Ip                     string
	Nodenumber     int
	Nodenumberkey   int
	Cdnnodeid        int
	Nodename         string
	Type              int
	Othercdnnodeid int
	Hdfsname         string
	Dispatchstatus      int
	Explain               string
	Act                    int
}

func OperateUploadNode(input *InputNodePara) string {
	session, db := conf.Init("mgo.conf")

	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	table := "c_node"
	collection := db.C(table)
	result := "{"

	switch input.Op {
	case conf.INSERT:
		// 插入数据
		e := new(conf.NodeInfo)
		e.Id_ = bson.NewObjectId()
		e.Ip = input.Ip
		e.Nodenumber = input.Nodenumber
		e.Nodename = input.Nodename
		e.Cdnnodeid = input.Cdnnodeid
		e.Type = input.Type
		// e.Hdfsname = input.Hdfsname
		// e.Dispatchstatus = input.Dispatchstatus
		// e.Act = input.Act
		e.Explain = input.Explain

		err := collection.Insert(&e)
		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			result = result + "\"result\":" +  "\"true\"" + "}"
		}
	case conf.SELECT:
		// 查询数据
		result = result + "\"result\": " + "["
		// e := new(conf.NodeInfo)
		// err := collection.Find(bson.M{"nodenumber": input.Nodenumber}).One(&e)
		var info[] conf.NodeInfo
		err  := collection.Find(bson.M{"nodenumber": input.Nodenumber}).All(&info)

		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			for i ,_ := range info {
				// fmt.Println(info[i])
				e := info[i];
				result = result + "{" + "\"id\":" + strconv.Itoa(e.Id) + "," + "\"namenodeip\":" + "\"" + e.Namenodeip + "\"" + "," + "\"nodenumber\":" + strconv.Itoa(e.Nodenumber) + ","
				result = result + "\"nodename\":" + "\"" + e.Nodename + "\"" + "," + "\"hdfsname\":" + "\"" + e.Hdfsname + "\"" + ","
				result = result + "\"hdfsUrl\":" + "\"" + e.HdfsURL + "\"" + "," + "\"runstatus\":" + strconv.Itoa(e.Runstatus) + "," + "\"status\":" + strconv.Itoa(e.Status) + "," + "\"uploadstatus\":" + strconv.Itoa(e.Uploadstatus) + ","
				result = result + "\"encodestatus\":" + strconv.Itoa(e.Encodestatus) + "," + "\"cdnnodeid\":" + strconv.Itoa(e.Cdnnodeid) + "," + "\"ip\":" + "\"" + e.Ip + "\"" + "," + "\"cachesize\":" + "\"" + e.Cachesize + "\"" + "," + "\"cacheuse\":" + strconv.Itoa(e.Cacheuse) + ","
				result = result + "\"beattime\":" + "\"" + e.Beattime + "\"" + "," + "\"dispatchstatus\":" + strconv.Itoa(e.Dispatchstatus) + "," + "\"explain\":" + "\"" + e.Explain + "\"" + "," + "\"act\":" + strconv.Itoa(e.Act) + "," + "\"downloadloadbalanceurl\":" + "\"" + e.DownloadLoadbalanceURL + "\"" + "," 
				result = result + "\"type\":" + strconv.Itoa(e.Type) + "},"
			}
			result = strings.TrimSuffix(result, ",")
			result = result+ "]" + "," + "\"count\":" + strconv.Itoa(len(info)) + "}"
		}
	case conf.SELECTALL:
		// 查询all
		// var AllInfo conf.UploadServerAll;
		result = result + "\"result\": " + "["
		e := conf.NodeInfo{}
		iter := collection.Find(nil).Iter()
		var count = 0;
		for iter.Next(&e) {
			count++;
			result = result + "{" + "\"id\":" + strconv.Itoa(e.Id) + "," + "\"namenodeip\":" + "\"" + e.Namenodeip + "\"" + "," + "\"nodenumber\":" + strconv.Itoa(e.Nodenumber) + ","
			result = result + "\"nodename\":" + "\"" + e.Nodename + "\"" + "," + "\"hdfsname\":" + "\"" + e.Hdfsname + "\"" + ",";
			result = result + "\"hdfsUrl\":" + "\"" + e.HdfsURL + "\"" + "," + "\"runstatus\":" + strconv.Itoa(e.Runstatus) + "," + "\"status\":" + strconv.Itoa(e.Status) + "," + "\"uploadstatus\":" + strconv.Itoa(e.Uploadstatus) + ","
			result = result + "\"encodestatus\":" + strconv.Itoa(e.Encodestatus) + "," + "\"cdnnodeid\":" + strconv.Itoa(e.Cdnnodeid) + "," + "\"ip\":" + "\"" + e.Ip + "\"" + "," + "\"cachesize\":" + "\"" + e.Cachesize + "\"" + "," + "\"cacheuse\":" + strconv.Itoa(e.Cacheuse) + ","
			result = result + "\"beattime\":" + "\"" + e.Beattime + "\"" + "," + "\"dispatchstatus\":" + strconv.Itoa(e.Dispatchstatus) + "," + "\"explain\":" + "\"" + e.Explain + "\"" + "," + "\"act\":" + strconv.Itoa(e.Act) + "," + "\"downloadloadbalanceurl\":" + "\"" + e.DownloadLoadbalanceURL + "\"" + "," 
			result = result + "\"type\":" + strconv.Itoa(e.Type) + "},"
  		}
  		result = strings.TrimSuffix(result, ",")
  		result = result+ "]" + "," + "\"count\":" + strconv.Itoa(count) + "}"
	case conf.UPDATE:
		// 更新数据
		err := collection.Update(bson.M{"nodenumber": input.Nodenumberkey}, 
			bson.M{"$set": bson.M{"ip": input.Ip, "nodenumber": input.Nodenumber, "explain": input.Explain,
			"cdnNodeId": input.Cdnnodeid, "nodename": input.Nodename, "type": input.Type}})
		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			result = result + "\"result\":" +  "\"true\"" + "}"
		}
	case conf.DELETE:
		// 删除数据
		_, err := collection.RemoveAll(bson.M{"nodenumber": input.Nodenumber})
		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			result = result + "\"result\":" +  "\"true\"" + "}"
		}
	default:
		info := fmt.Sprintf("Invalid op = %d", input.Op)
		fmt.Println(info)
		result = result + "\"result\":" +  "\"false\"" + "}"
	}

	return result
}
