package main

import (
	"fmt"
	"github.com/hpcloud/tail"
)

func main() {
	t, _ := tail.TailFile("/data/fastdfs/storage/logs/storage_access.log", tail.Config{Follow: true})
	for line := range t.Lines {
		fmt.Println(line.Text)
	}
}
