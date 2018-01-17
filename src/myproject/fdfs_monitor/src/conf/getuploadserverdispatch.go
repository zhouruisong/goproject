package conf
//
//import (
////	"strconv"
////	"github.com/labix.org/v2/mgo"
////	"github.com/labix.org/v2/mgo/bson"
//)
//
//func GetUploadServerDispatch() string {
////	session, db := conf.Init("mgo.conf")
////
////	defer session.Close()
////	session.SetMode(mgo.Monotonic, true)
////
////	table := conf.Read("upload", "table")
////	collection := db.C(table)
////	info := new(conf.UploadServerInfo)
////	err := collection.Find(bson.M{"ip": "127.0.0.1"}).One(&info)
////	result := "{"
////	if err != nil {
////		//panic(err)
////		result = result + "\"result\":" +  "\"false\"" + "}"
////	} else {
////		result = result + "\"act\":" + strconv.Itoa(info.Act) + "," + "\"dispatchstatus\":" + strconv.Itoa(info.Dispatchstatus) +  "}"
////	}
//
//	return 0
//}