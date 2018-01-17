package main

import (
	"./tbs"
	// "fmt"
	"time"
)

func main() {
	var cb tbs.EventCallback = onTest
	//获取分派器单例
	tbs.SetCallBack(&cb)
	
	service := ":1200"
	tbs.ServerStarted(service)

	//因为主线程不会等子线程而直接关闭进程，这样会看不到效果，所以我在这里加了阻塞式延时
	time.Sleep(time.Second * 1)
}

//回调出得到的就是一个event对象了
func onTest(event *tbs.Event) {
	// fmt.Println("onTest", event.Params["id"])
}
