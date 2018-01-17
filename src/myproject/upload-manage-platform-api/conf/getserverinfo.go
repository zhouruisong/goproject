
package conf

import (
	"./conf"
	// "fmt"
)


func ReadConfig() string {
	url := conf.ReadServerConf("mgo.conf")
	// fmt.Println(url)
	return url
}
