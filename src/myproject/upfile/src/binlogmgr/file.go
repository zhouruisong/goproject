package binlogmgr

import (
	"os"
)

func (mgr *BinLogMgr) FileOpen(name string) *os.File {
	pFile, err := os.OpenFile(name, os.O_RDWR, 0777)
	if err != nil {
		mgr.Logger.Errorf("Open:%v failed", name)
		defer pFile.Close()
		return nil
	}

	return pFile
}
