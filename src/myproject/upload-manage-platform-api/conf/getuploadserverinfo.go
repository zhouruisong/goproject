// 获取指定节点的所有上传机的信息

package conf

import (
	"./conf"
	"strconv"
	"strings"
	"github.com/labix.org/v2/mgo"
	"github.com/labix.org/v2/mgo/bson"
)

func GetUploadServerInfo(nodenum int) string {
	session, db := conf.Init("mgo.conf")

	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	table := conf.Read("upload", "table")
	collection := db.C(table)
	e := new(conf.UploadServerInfo)
	err := collection.Find(bson.M{"nodenumber": nodenum}).One(&e)
	result := "{"
	if err != nil {
		result = result + "\"result\":" +  "\"false\"" + "}"
	} else {
		result = result + "\"result\": " + "["
		result = result + "{" + "\"act\":" + strconv.Itoa(e.Act) + "," + "\"beattime\":" + "\"" + e.Beattime + "\"" + "," + "\"cachesize\":" + "\"" + e.Cachesize + "\"" + ","
		result = result + "\"cacheuse\":" + strconv.Itoa(e.Cacheuse) + "," + "\"dispatchstatus\":" + strconv.Itoa(e.Dispatchstatus) + ","
		result = result + "\"explain\":" + "\"" + e.Explain + "\"" + "," + "\"id\":" + strconv.Itoa(e.Id) + "," + "\"ip\":" + "\"" + e.Ip + "\"" + "," + "\"nodenumber\":" + strconv.Itoa(e.Nodenumber) + ","
		result = result + "\"runstatus\":" + strconv.Itoa(e.Runstatus) + "},"
		result = strings.TrimSuffix(result, ",")
		result = result+ "]}"
	}

	return result
}
