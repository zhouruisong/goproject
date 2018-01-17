package tair

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
)

type TairClient struct {
	Logger     *log.Logger
	TairServer string
	Tairclient string
	FdfsBackup string
}

func NewTairClient(server []string, tairclient string, fdfsbackup string, lg *log.Logger) *TairClient {
	var sever_addr string
	if len(server) == 2 {
		sever_addr = server[0] + "," + server[1]
	} else if len(server) == 1 {
		sever_addr = server[0]
	} else {
		fmt.Println("ERROR: tair_server len: %d", len(server))
		return nil
	}

	c := &TairClient{
		Logger:     lg,
		TairServer: sever_addr,
		Tairclient: tairclient,
		FdfsBackup: fdfsbackup,
	}
	c.Logger.Infof("NewTairClient ok")
	return c
}
