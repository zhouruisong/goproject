package cluster_sync

import (
	"../protocal"
	"crypto/md5"
	"fmt"
	"io"
	//	"io/ioutil"
	"math"
	"os"
	//	"strconv"
	"strings"
	//"sync"
	"time"
)

/* 二级索引格式如下：
52428800
my15286.mp4_0 group1/M02/00/0F/wKhuHlka4U6AQNtVAKAAAHBWCyc8.mp4_0 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_1 group1/M03/00/0F/wKhuHlka4U6AfX2rAKAAAHBWCyc3.mp4_1 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_2 group1/M04/00/0F/wKhuHlka4U6AEkY7AKAAAHBWCyc8.mp4_2 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_3 group1/M05/00/0F/wKhuHlka4U6AbFFWAKAAAHBWCyc9.mp4_3 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_4 group1/M00/00/0F/wKhuHlka4U6ANyjJAKAAAHBWCyc4.mp4_4 0 10485760 dff93aed140e1b0584ba6aabfb491061
*/

//切割文件上传
func (sy *SyncMgr) SplitFileUpload(data *protocal.UploadFile) (int, string) {
	defer func() {
		sy.removeFile(data.Fpath)
	}()

	if data.Taskid == "" || data.Domain == "" || data.Uri == "" || data.Behavior == "" || data.Fname == "" || data.Ftype == "" || data.Fpath == "" || data.CbUrl == "" || data.FileSize <= 0 {
		sy.Logger.Errorf("have null paramater!")
		return 1, "upload failed"
	}

	//open file
	file, err := os.OpenFile(data.Fpath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		sy.Logger.Errorf("open file %+v failed", data.Fpath)
		return 1, "upload failed"
	}

	defer file.Close()

	taskid := data.Taskid
	seed := fmt.Sprintf("%ld", time.Now().UnixNano())
	seedbuf := []byte(seed)
	subTaskid := fmt.Sprintf("%x", md5.Sum(seedbuf))
	subTaskid = strings.ToUpper(subTaskid)

	finfo, err := file.Stat()
	if err != nil {
		sy.Logger.Errorf("taskid:%+v,subTaskid:%+v,get file info failed from %+v",
			taskid, subTaskid, data.Fpath)
		return 1, "upload failed"
	}

	size := data.FileSize
	if finfo.Size() != size {
		sy.Logger.Errorf("taskid:%+v,subTaskid:%+v,size is not equal %+v",
			taskid, subTaskid, data.Fpath)
		return 1, "upload failed"
	}
	//每次最多拷贝10m
	bufsize := int64(1024 * 1024 * 10)
	if size < bufsize {
		bufsize = size
	}

	//向上向下取整数
	num := int64(math.Ceil(float64(finfo.Size()) / float64(bufsize)))
	newContent := fmt.Sprintf("%v", size)
	sy.Logger.Infof("taskid:%+v,size:%+v,slice num:%+v", taskid, size, num)

	indexMap := protocal.IndexCache{
		FileSize: newContent,
		Item:     make([]protocal.IndexInfo, num),
	}

	buf := make([]byte, bufsize)
	Finch := make(chan int, num)

	var i int64 = 0
	//slice upload
	for ; i < num; i++ {
		file.Seek(i*int64(bufsize), 0)

		//剩余不够切片，缩减buff大小
		if int64(len(buf)) > (size - i*bufsize) {
			//sy.Logger.Infof("i= %+v,num= %+v,buflen:%+v,left:%+v", i, num, len(buf), size-i*bufsize)
			buf = make([]byte, size-i*bufsize)
		}

		n, err2 := file.Read(buf)
		if err2 != nil && err2 != io.EOF {
			sy.Logger.Errorf("err2:%+v failed to read from %+v", err2, file)
			return 1, "failed to read"
		}
		if n <= 0 {
			sy.Logger.Errorf("n <= 0 failed read from %+v", err2, file)
			return 1, "failed to read"
		}

		copylen := int64(len(buf))

		//刷新索引内容
		newfilename := data.Fname + "." + data.Ftype + "_" + fmt.Sprintf("%06v", i)
		indexMap.Item[i].Name = newfilename
		indexMap.Item[i].Id = ""
		indexMap.Item[i].Status = "0"
		indexMap.Item[i].Size = fmt.Sprintf("%v", copylen)
		indexMap.Item[i].Md5 = fmt.Sprintf("%x", md5.Sum(buf))

		msg := &protocal.Ctx{
			Number:    i,
			Length:    num,
			Taskid:    taskid,
			SubTaskid: subTaskid,
			Cache:     &indexMap,
			Data:      make([]byte, copylen),
			ResCh:     Finch,
		}

		copy(msg.Data, buf)
		sy.FdfsCh <- msg //写入管道
	}

	//判断每个片上传的返回结果
	var k int64 = 0
	isFinished := true
	for ; k < num; k++ {
		flag, isClose := <-Finch
		if !isClose {
			sy.Logger.Errorf("channel closed!")
			return -1, "upload failed"
		}
		if flag == 0 {
			sy.Logger.Infof("taskid:%+v,have failed slice upload", taskid)
			isFinished = false
		}
	}

	//有上传失败的，删除成功的部分
	if isFinished == false {
		sy.Logger.Errorf("ready to removeBuff")
		sy.removeBuff(taskid, subTaskid, &indexMap, num)
		return 1, "upload failed"
	}

	//sy.Logger.Infof("taskid:%+v,subTaskid:%+v,upload all slice file successful",
	//	taskid, subTaskid)

	//将二级索引文件上传到备份集群的fastdfs并且存储id到备份集群的tair
	var newindex string
	filesize := fmt.Sprintf("%s\n", indexMap.FileSize)
	newindex = newindex + filesize
	for i := int64(0); i < num; i++ {
		line := fmt.Sprintf("%s %s %s %s %s\n", indexMap.Item[i].Name, indexMap.Item[i].Id,
			indexMap.Item[i].Status, indexMap.Item[i].Size, indexMap.Item[i].Md5)
		newindex = newindex + line
	}

	//sy.Logger.Infof("new index:\n%s", newindex)

	// 存储新的二级索引内容到备份集群的fdfs中
	indexbuf := []byte(newindex)
	id := sy.process(indexbuf, taskid, subTaskid, 0, true)
	if id == "" {
		//索引上传失败，需要删除索引对应的内容
		sy.Logger.Errorf("taskid:%+v,subTaskid:%+v,index upload failed,file:%+v",
			taskid, subTaskid, data.Fpath)
		sy.removeBuff(taskid, subTaskid, &indexMap, num)
		return 1, "upload failed"
	}

	//sy.Logger.Infof("taskid:%+v,subTaskid:%+v,upload index successful", taskid, subTaskid)

	//将返回的索引id存储到备份集群的tair中
	result := sy.putIndexFile(data, id)
	if result != 0 {
		// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
		sy.removeBuff(taskid, subTaskid, &indexMap, num)
		rt := sy.pTran.Deletebuff(taskid, subTaskid, 0, id)
		if rt != 0 {
			sy.Logger.Errorf("delete data from standby fdfs failed,taskid:%+v,subTaskid:%+v",
				taskid, subTaskid)
		}

		return 1, "upload failed"
	}

	//sy.Logger.Infof("taskid:%+v,subTaskid:%+v,upload index id successful", taskid, subTaskid)
	//sy.Logger.Infof("taskid:%+v,subTaskid:%+v,upload finish", taskid, subTaskid)

	return 0, "success"
}

func (sy *SyncMgr) removeBuff(taskid string,subTaskid string,indexMap *protocal.IndexCache, num int64) {
	// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
	for j := int64(0); j < num; j++ {
		index := indexMap.Item[j].Id
		if index == "" {
			continue
		}
		// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
		rt := sy.pTran.Deletebuff(taskid, subTaskid, int(j), index)
		if rt != 0 {
			sy.Logger.Errorf("delete data from standby fdfs failed,taskid:%+v,subTaskid:%+v",
				taskid, subTaskid)
		}
	}
}

func (sy *SyncMgr) removeFile(filename string) {
	err := os.Remove(filename) //删除文件
	if err != nil {
		sy.Logger.Errorf("file remove Error:%+v", err)
	} else {
		sy.Logger.Infof("%+v remove successful", filename)
	}
}

func (sy *SyncMgr) cunsumeFdfsCh() {
	for {
		ctx, isClose := <-sy.FdfsCh
		if !isClose {
			sy.Logger.Errorf("channel closed!")
			return
		}

		i := ctx.Number
		length := ctx.Length
		taskid := ctx.Taskid
		subTaskid := ctx.SubTaskid
		buf := ctx.Data
		ResIndexMap := ctx.Cache

		ResIndexMap.Item[i].Id = sy.process(buf, taskid, subTaskid, int(i), false)
		//返回空有问题，直接跳过这个任务
		if ResIndexMap.Item[i].Id == "" {
			sy.Logger.Errorf("slice upload failed,taskid: %+v,slice: %+v", taskid, i)
			sy.WriteResChResult(ctx, 0)
			continue
		}

		//sy.Logger.Infof("%v", ResIndexMap.Item[i])

		isFinished := true
		for j := int64(0); j < length; j++ {
			if ResIndexMap.Item[j].Id == "" {
				isFinished = false
				break
			}
		}

		if !isFinished {
			sy.WriteResChResult(ctx, 1)
			continue
		}

		sy.WriteResChResult(ctx, 1)
	}
	return
}

func (sy *SyncMgr) WriteResChResult(ctx *protocal.Ctx, flag int) int {
	select {
	case ctx.ResCh <- flag:
		//sy.Logger.Infof("WriteResChResult successful,flag:%+v", flag)
		return 0
	case <-time.After(time.Second * 1):
		sy.Logger.Errorf("write to channel timeout, slice upload failed,taskid: %+v,slice: %+v", ctx.Taskid, ctx.Number)
		return -1
	}
	return 0
}
