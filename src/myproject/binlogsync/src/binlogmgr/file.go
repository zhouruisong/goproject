package binlogmgr

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// 从缓存获取id读应的文件标识符
func (mgr *BinLogMgr) GetFileFd(name string) *os.File {
	if v, ok := g_LastOkIdFileMap[name]; ok {
		return v
	} else {
		return nil
	}
}

// 文件标识符存入缓存
func (mgr *BinLogMgr) InsertFileMap(name string, f *os.File) int {
	g_LastOkIdFileMap[name] = f
	return 0
}

// 每个用户表创建一个fd，用于保存最后收到的id，
// 程序在启动的时候会获取所有大于这个id的任务，发送到备份集群上传机
func (mgr *BinLogMgr) FileCreate(name string) (error, *os.File) {
	filename := g_lastidfile_path + "/" + name
	pFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		mgr.Logger.Errorf("Open:%v failed", filename)
		defer pFile.Close()
		return err, nil
	}

	return nil, pFile
}

func (mgr *BinLogMgr) FileOpen(name string) *os.File {
	pFile, err := os.OpenFile(name, os.O_RDWR, 0777)
	if err != nil {
		mgr.Logger.Errorf("Open:%v failed", name)
		defer pFile.Close()
		return nil
	}

	return pFile
}

func (mgr *BinLogMgr) IndexFileCreate(name string) (*os.File, string) {
	filename := g_lastidfile_path + "/index/" + name
	pFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		mgr.Logger.Errorf("Open:%v failed", filename)
		defer pFile.Close()
		return nil, ""
	}

	return pFile, filename
}

func (mgr *BinLogMgr) IndexFileRemove(name string) error {
	return os.Remove(name)
}

//获取指定目录下的所有文件，不进入下一级目录搜索，可以匹配后缀过滤。
func (mgr *BinLogMgr) ListDir(dirPth string) (files []string, err error) {
	files = make([]string, 0, 10)

	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}

	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}

		files = append(files, fi.Name())
	}

	return files, nil
}

func (mgr *BinLogMgr) FileInfoRead() error {
	files, err := mgr.ListDir(g_lastidfile_path)
	if err != nil {
		mgr.Logger.Errorf("ListDir failed, path: %+v", g_lastidfile_path)
		return err
	}

	for _, fi := range files {
		err := mgr.readFileInfo(fi)
		if err != nil {
			mgr.Logger.Errorf("readFileInfo failed, filename: %+v", fi)
			return err
		}
	}

	return nil
}

func (mgr *BinLogMgr) readFileInfo(name string) error {
	mgr.Logger.Infof("table name: %+v", name)
	err, fd := mgr.FileCreate(name)
	if err != nil {
		mgr.Logger.Errorf("fileCreate:%v failed", name)
		return err
	}

	ret := mgr.InsertFileMap(name, fd)
	if ret != 0 {
		mgr.Logger.Errorf("InsertFileMap failed name:%+v", name)
		return err
	}

	err = mgr.LastIdFileRead(fd)
	if err != nil {
		mgr.Logger.Errorf("LastIdFileRead failed err:%+v ", err)
		return err
	}

	return nil
}

// 读取每个用户表的最后id，获取所有大于这个id的任务，发送到备份集群上传机
func (mgr *BinLogMgr) LastIdFileRead(f *os.File) error {
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			return nil
		}

		fmt.Println(line)
		lineslice := strings.Split(line, ",")
		if len(lineslice) == 0 {
			return nil
		}

		b, errs := strconv.Atoi(lineslice[1])
		if errs != nil {
			return nil
		}

		// 每个用户表对应一个最后成功接收的id，
		// 程序启动时获取所有大于这个id的任务，发送到备份集群上传机
		g_LastOkIdCache[lineslice[0]] = b
		mgr.Logger.Infof("last receive id: %+v", b)

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

	return nil
}

func (mgr *BinLogMgr) PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//最后一次接收成功的id写到文件中
func (mgr *BinLogMgr) WriteTaskIdTofile(id int, tablename string) int {
	f := mgr.GetFileFd(tablename)
	if f == nil {
		mgr.Logger.Infof("%v not exist, need create", tablename)
		// 创建失败任务表，用于定时器循环失败任务，表存在不创建
		if mgr.pSql.CreateFailedTable(tablename) != 0 {
			mgr.Logger.Errorf("CreateFailedTable failed name: %+v", tablename)
			return -1
		}

		// 创建用于存储最后成功接收id的文件
		err, fd := mgr.FileCreate(tablename)
		if err != nil {
			mgr.Logger.Errorf("fileCreate %v failed", tablename)
			return -1
		}

		// 插入map中
		if mgr.InsertFileMap(tablename, fd) != 0 {
			mgr.Logger.Errorf("InsertFileMap failed name: %+v", tablename)
			return -1
		}

		f = fd
	}

	// 清除文件内容
	filename := g_lastidfile_path + "/" + tablename
	//mgr.Logger.Infof("filename:%+v", filename)
	err := os.Truncate(filename, 0)
	if err != nil {
		mgr.Logger.Errorf("Truncate failed, err:%+v", err)
		return -1
	}

	newContent := fmt.Sprintf("%s,%d", tablename, id)
	//fd_content := strings.Join([]string{newContent, "\n"}, "")
	//mgr.Logger.Infof("newContent:%+v", newContent)
	buf := []byte(newContent)
	f.Write(buf)
	//n, _ := f.Write(buf)
	//mgr.Logger.Infof("n:%+v", n)
	f.Sync()

	return 0
}
