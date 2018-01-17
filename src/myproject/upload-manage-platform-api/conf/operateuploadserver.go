// 上传机的CRUD接口

package conf

import (
	"./conf"
	"fmt"
	"strconv"
	"strings"
	"github.com/labix.org/v2/mgo"
	"github.com/labix.org/v2/mgo/bson"
)

type InputServerPara struct {
	Op                     int
	Nodenumber           int
	Nodenumberkey       int
	Ip                     string
	Explain               string
	Id_                    string
	Dispatchstatus      int
	Act                    int
}

func OperateUploadServer(input *InputServerPara) string {
	session, db := conf.Init("mgo.conf")

	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	table := conf.Read("upload", "table")
	collection := db.C(table)
	result := "{"
	switch input.Op {
	case conf.INSERT:
		// 插入数据
		info := new(conf.UploadServerInfo)
		info.Id_ = bson.NewObjectId()
		info.Ip = input.Ip
		info.Act = input.Act
		info.Explain = input.Explain
		info.Nodenumber = input.Nodenumber
		info.Dispatchstatus = input.Dispatchstatus
		fmt.Println(info)

		err := collection.Insert(&info)
		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			result = result + "\"result\":" +  "\"true\"" + "}"
		}
	case conf.SELECT:
		// 查询数据
		result = result + "\"result\": " + "["
		var info[] conf.UploadServerInfo

		err  := collection.Find(bson.M{"nodenumber": input.Nodenumber}).All(&info)
		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			for i ,_ := range info {
				// fmt.Println(info[i])
				e := info[i];
				result = result + "{" + "\"act\":" + strconv.Itoa(e.Act) + "," + "\"beattime\":" + "\"" + e.Beattime + "\"" + "," + "\"cachesize\":" + "\"" + e.Cachesize + "\"" + ","
				result = result + "\"cacheuse\":" + strconv.Itoa(e.Cacheuse) + "," + "\"dispatchstatus\":" + strconv.Itoa(e.Dispatchstatus) + ","
				result = result + "\"explain\":" + "\"" + e.Explain + "\"" + "," + "\"id\":" + strconv.Itoa(e.Id) + "," + "\"ip\":" + "\"" + e.Ip + "\"" + "," + "\"nodenumber\":" + strconv.Itoa(e.Nodenumber) + ","
				result = result + "\"runstatus\":" + strconv.Itoa(e.Runstatus) + "},"
			}
			result = strings.TrimSuffix(result, ",");
			result = result+ "]" + "," + "\"count\":" + strconv.Itoa(len(info)) + "}";
		}
	case conf.SELECTALL:
		// 查询all
		// var AllInfo conf.UploadServerAll;
		result = result + "\"result\": " + "["
		e := conf.UploadServerInfo{};
		iter := collection.Find(nil).Iter();
		var count = 0;
		for iter.Next(&e) {
			count++;
    			result = result + "{" + "\"Id_\":" + "\"" + e.Id_.Hex() + "\"" + "," + "\"act\":" + strconv.Itoa(e.Act) + "," + "\"beattime\":" + "\"" + e.Beattime + "\"" + "," + "\"cachesize\":" + "\"" + e.Cachesize + "\"" + ",";
			result = result + "\"cacheuse\":" + strconv.Itoa(e.Cacheuse) + "," + "\"dispatchstatus\":" + strconv.Itoa(e.Dispatchstatus) + ",";
			result = result + "\"explain\":" + "\"" + e.Explain + "\"" + "," + "\"id\":" + strconv.Itoa(e.Id) + "," + "\"ip\":" + "\"" + e.Ip + "\"" + "," + "\"nodenumber\":" + strconv.Itoa(e.Nodenumber) + ",";
			result = result + "\"runstatus\":" + strconv.Itoa(e.Runstatus) + "},";
  		}
  		result = strings.TrimSuffix(result, ",");
  		result = result+ "]" + "," + "\"count\":" + strconv.Itoa(count) + "}";
  		// fmt.Println(AllInfo)
	case conf.UPDATE:
		// 更新数据
		// fmt.Println(input)
		err := collection.Update(bson.M{"nodenumber": input.Nodenumberkey}, 
			bson.M{"$set": bson.M{"ip": input.Ip, "nodenumber": input.Nodenumber, "act": input.Act, "dispatchstatus": input.Dispatchstatus, "explain": input.Explain}})
		if err != nil {
			result = result + "\"result\":" +  "\"false\"" + "}"
		} else {
			result = result + "\"result\":" +  "\"true\"" + "}"
		}
	case conf.DELETE:
		// 删除数据
		// objectId := bson.ObjectIdHexString(input.Id_)
		// fmt.Println(objectId)
		// _, err := collection.RemoveAll(bson.M{"_id": bson.ObjectId(input.Id_)})
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
