package conf

import (
	"./conf"
	// "fmt"
)

func LoginCheck(name, pwd string) int {
	session, _ := conf.Init("mgo.conf")
	session.Close()
	
	username := conf.Read("login", "username")
	password := conf.Read("login", "password")
	// fmt.Println(username)
	// fmt.Println(password)

	if (name != username) {
		return 1
	} else if (password != pwd) {
		return 2
	} else {
		return 0
	}
}