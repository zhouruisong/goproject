package clnMgr

import (
	_ "../common"
	"../devMgr"
	"../ruleMgr"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	//"os"
	"errors"
	"math/rand"
	"sync"
	"time"
)

type ClnMgr struct {
	hosts    []string
	devmgr   *devMgr.DevMgr
	rulemgr  *ruleMgr.RuleMgr
	devs     []devMgr.DevInfo
	cls      []devMgr.ClusterInfo
	rules    []ruleMgr.RuleInfo
	cleaners []*Cleaner
	Logger   *log.Logger
	cleaner  *Cleaner
	interval int64
}

func NewClnMgr(dm *devMgr.DevMgr, rm *ruleMgr.RuleMgr, lg *log.Logger, sec int64) *ClnMgr {
	dm.LoadDevInfos()
	dm.LoadClusterInfos()
	rm.LoadRuleInfos()
	c := &ClnMgr{
		devmgr:  dm,
		rulemgr: rm,
		devs:    dm.DevInfos,
		cls:     dm.ClusterInfos,
		rules:   rm.RuleInfos,
		Logger:  lg,
		interval sec,
	}
	return c
}

func (cm *ClnMgr) Restart() {
	cm.Stop()
	time.Sleep(2 * time.Second)
	cm.devmgr.LoadDevInfos()
	cm.devmgr.LoadClusterInfos()
	cm.rulemgr.LoadRuleInfos()
	cm.devs = cm.devmgr.DevInfos
	cm.cls = cm.devmgr.ClusterInfos
	cm.rules = cm.rulemgr.RuleInfos
	cm.Run()
}

//服务线程启动
func (cm *ClnMgr) Run() {
	//遍历规则，为每个规则创建一个cleaner
	for _, r := range cm.rules {
		c := NewCleaner(cm.Logger, r, cm.devs, cm.cls, cm.interval)
		cm.cleaners = append(cm.cleaners, c)
		go c.Run()
	}
	//单独的清理，用于做指定文件删除
	if len(cm.rules) > 0 {
		cm.cleaner = NewCleaner(cm.Logger, cm.rules[0], cm.devs, cm.cls, cm.interval)
	}
}

func (cm *ClnMgr) Stop() {
	for _, c := range cm.cleaners {
		c.Stop()
	}
}

func (cm *ClnMgr) CleanFile(logid string, buf []byte) error {
	if cm.cleaner == nil {
		return errors.New("no cleaner to clean file.")
	}
	return cm.cleaner.CleanFile(logid, buf)
}

func (cm *ClnMgr) CleanStreamFile(logid string, buf []byte) error {
	if cm.cleaner == nil {
		return errors.New("no cleaner to clean file.")
	}
	return cm.cleaner.CleanStreamFile(logid, buf)
}

func (cMgr *ClnMgr) initialize() {
	//根据给的规则生成多个cleaner
}

//为每个删除规则定义一个cleaner
type Cleaner struct {
	name      string
	coustomer []string         //bilibili等名称
	host      []string         //注册的设备地址列表
	rule      ruleMgr.RuleInfo //清理策略
	devs      []devMgr.DevInfo //获取索引文件的地址
	cls       []devMgr.ClusterInfo
	logger    *log.Logger
	stop_ch   chan bool
	interval  int64
	initial   bool
}

func NewCleaner(lg *log.Logger, r ruleMgr.RuleInfo, ds []devMgr.DevInfo, cs []devMgr.ClusterInfo, sec int64) *Cleaner {
	//cleaner
	cl_name := fmt.Sprintf("cleaner_%s", r.Name)
	c := &Cleaner{
		name:    cl_name,
		rule:    r,
		devs:    ds,
		cls:     cs,
		logger:  lg,
		stop_ch: make(chan bool, len(cs)),
		interval: sec,
	}
	return c
}

func (clr *Cleaner) Run() {
	//为每个集群创建服务线程service
	for _, c := range clr.cls {
		ch := make(chan TaskInfo, 100)
		go clr.service_get_m3u8(c, ch)
		go clr.service(c, ch)
	}
}

func (clr *Cleaner) Stop() {
	for i := 0; i < len(clr.cls); i++ {
		clr.stop_ch <- true
	}
}

func (clr *Cleaner) getLocalTimeClock() (hour, min, sec int) {
	now := time.Now()
	hour, min, sec = now.Clock()
	return
}

type MediaInfo struct {
	Id    int    `json:"id"`
	Vhost string `json:"vhost"`
	File  string `json:"file"`
}

type TaskInfo struct {
	Info  []MediaInfo
	Table string
}

//获取删除索引接口返回值
type MediaInfoRet struct {
	Errno  int         `json:"errno"`
	Errmsg string      `json:"errmsg"`
	Table  string      `json:"table"`
	Data   []MediaInfo `json:"data"`
}

//发送给清理设备的命令结构
type CleanOder struct {
	Table string      `json:"table"`
	Data  []MediaInfo `json:"data"`
}

//获取集群下的所有存储设备信息
func (clr *Cleaner) getDevsByCls(cls *devMgr.ClusterInfo) []devMgr.DevInfo {
	var devlist []devMgr.DevInfo = make([]devMgr.DevInfo, 0)
	for _, d := range clr.devs {
		if cls.Name == d.Cluster_name {
			devlist = append(devlist, d)
		}
	}
	return devlist
}

type QueryM3u8 struct {
	Customer string `json:"customer"`
	Expire   int    `json:"expire"`
	Size     int    `json:"size"`
}

//向集群mysql服务查询需要删除的m3u8
func (clr *Cleaner) queryFromClusterByHttp(host string, r *ruleMgr.RuleInfo) (error, []byte) {
	q := QueryM3u8{
		Customer: r.Table_names,
		Expire:   r.Expire_time,
		Size:     r.Batch_size,
	}

	b, err := json.Marshal(q)
	if err != nil {
		clr.logger.Errorf("json encode failed:%v.", err)
		return err, nil
	}

	clr.logger.Infof("get m3u8 list from %v:%v", host, string(b))
	body := bytes.NewBuffer([]byte(b))
	var url string
	if clr.initial {
		url = fmt.Sprintf("http://%v/inquire_expired", host)
	} else {
		url = fmt.Sprintf("http://%v/inquire_wait_delete", host)
	}
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.logger.Errorf("http post return failed.err:%v", err)
		return err, nil
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.logger.Errorf("ioutil readall failed.err:%v", err)
		return err, nil
	}
	clr.initial = true
	return nil, result
}

//获取需要清理的m3u8列表
func (clr *Cleaner) getCleanList(cls *devMgr.ClusterInfo) *MediaInfoRet {
	err, b := clr.queryFromClusterByHttp(cls.Host, &clr.rule)
	if err != nil {
		clr.logger.Errorf("queryFromClusterByHttp return null.cls:%v, host:%v, err:%v", cls.Name, cls.Host, err)
		return nil
	}

	var ret MediaInfoRet
	err = json.Unmarshal(b, &ret)
	if err != nil {
		clr.logger.Errorf("json decode failed:%v,b:%v", err, string(b))
		return nil
	}
	clr.logger.Infof("clean list:%+v", ret)
	return &ret
}

//向各存储机器发送清理文件指令
func (clr *Cleaner) cleanEachDevice(host string, table string, data []MediaInfo) string {
	o := CleanOder{
		Table: table,
		Data:  data,
	}

	b, err := json.Marshal(o)
	if err != nil {
		clr.logger.Errorf("json encode failed:%v,b:%v", err, string(b))
		return ""
	}

	clr.logger.Infof("send to clean device %v:%v", host, string(b))
	body := bytes.NewBuffer([]byte(b))
	res, err := http.Post(host, "application/json;charset=utf-8", body)
	if err != nil {
		clr.logger.Errorf("http to clean device %v failed,%v", host, err)
		return ""
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.logger.Errorf("%v cleanEachDevice ioutil failed, %v", host, err)
		return ""
	}
	//回复清除状态
	clr.logger.Infof("return from clean device %v: %s", host, result)
	return string(result)
}

type CleanStatus struct {
	Id  int `json:"id"`
	Res int `json:"res"`
}

type CleanStatusRet struct {
	Errno  int           `json:"errno"`
	Errmsg string        `json:"errmsg"`
	Table  string        `json:"table"`
	Data   []CleanStatus `json:"data"`
}

type CleanAck struct {
	Table string        `json:"table"`
	Data  []CleanStatus `json:"data"`
}

//回写数据库删除状态
func (clr *Cleaner) ackCleanStatu2Cluster(clrd string, host string, r string) int {

	var cr CleanStatusRet
	if r == "" {
		clr.logger.Errorf("%v ack to cluster %v result from clean device is nil.", clrd, host)
		return 0
	}
	err := json.Unmarshal([]byte(r), &cr)
	if err != nil {
		clr.logger.Errorf("%v ack to cluster %v decode json err:%v", clrd, host, err)
		return 0
	}

	if cr.Table == "" || len(cr.Data) == 0 {
		clr.logger.Errorf("%v clean device return null data,no need to ack cluster %v.", clrd, host)
		return 0
	}

	ca := CleanAck{
		Table: cr.Table,
		Data:  cr.Data,
	}

	b, err := json.Marshal(ca)
	if err != nil {
		clr.logger.Errorf("%v ack to cluster %v encode json err: %v", clrd, host, err)
		return 0
	}

	clr.logger.Infof("%v ack to cluster:%v", clrd, string(b))
	url := fmt.Sprintf("http://%s/update_status", host)
	clr.logger.Infof("update url:%v", url)
	body := bytes.NewBuffer([]byte(b))
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.logger.Errorf("http to cluster %v failed,%v", host, err)
		return -1
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.logger.Errorf("ackCleanStatu2Cluster %v ioutil failed,%v", host, err)
		return -1
	}
	//回复清除状态
	clr.logger.Infof("%v ack to cluster %v return :%s", clrd, host, result)
	return 0
}

func (clr *Cleaner) service_get_m3u8(cls devMgr.ClusterInfo, c chan TaskInfo) {
	for {
		select {
		case <-clr.stop_ch:
			clr.logger.Errorf("service_get_m3u8 stop and exit.")
			return
			//定时任务取m3u8文件列表
		case <-time.After(time.Second * clr.interval):
			hour, _, _ := clr.getLocalTimeClock()
			//	fmt.Println(hour, min, sec)
			//如果不在时间段sleep
			if hour < clr.rule.Start_time || hour >= clr.rule.End_time {
				clr.logger.Info("not in rule time zone.")
				time.Sleep(30 * time.Second)
				continue
			}
			//todo 访问接口获取需要删除的索引文件列表
			m := clr.getCleanList(&cls)
			if m == nil || len(m.Data) == 0 {
				//如果没有需要删除的文件
				clr.logger.Infof("no file need clean.")
				time.Sleep(20 * time.Second)
				continue
			}

			//将m3u8任务放入队列
			for _, info := range m.Data {
				task := TaskInfo{
					Table: m.Table,
				}
				task.Info = make([]MediaInfo, 0)
				task.Info = append(task.Info, info)
				c <- task
			}
		}
	}
}

func (clr *Cleaner) service(cls devMgr.ClusterInfo, c chan TaskInfo) {
	//todo 向该集群下的所有集群分发删除命令
	var waitgroup sync.WaitGroup
	devs := clr.getDevsByCls(&cls)
	dev_count := len(devs)
	if dev_count == 0 {
		clr.logger.Errorf("can't find devs to send clean order")
		return
	}

	for _, dev := range devs {
		for i := 0; i < clr.rule.Max_del_speed; i++ {
			waitgroup.Add(1)
			go clr.sendOder(c, cls.Host, dev, cls, &waitgroup)
		}
	}
	waitgroup.Wait()
	//clr.logger.Infof("cluster: %v@%v all send task done", cls.Name, cls.Host)
}

//向storage发送删除的列表
func (clr *Cleaner) sendOder(c chan TaskInfo, clshost string, d devMgr.DevInfo, cls devMgr.ClusterInfo, wg *sync.WaitGroup) {
	clr.logger.Info(" start send cleaner....")
	for {
		select {
		case <-clr.stop_ch:
			clr.logger.Errorf("stop and exit.")
			return
		case m := <-c:
			hour, _, _ := clr.getLocalTimeClock()
			//	fmt.Println(hour, min, sec)
			//如果不在时间段sleep
			if hour < clr.rule.Start_time || hour >= clr.rule.End_time {
				clr.logger.Info("send oder not in rule time zone.")
				time.Sleep(30 * time.Second)
				continue
			}

			url := fmt.Sprintf("http://%s/post_delete", d.Host)
			//clr.logger.Infof("clean task to %v", url)
			ret := clr.cleanEachDevice(url, m.Table, m.Info)
			if ret == "" {
				clr.logger.Errorf("%v@%v clean task failed", d.Name, d.Host)
				time.Sleep(2 * time.Second)
				continue
			}
			clr.ackCleanStatu2Cluster(d.Host, clshost, ret)
		}
	}
}

func (clr *Cleaner) queryClusterByStreamInfo(logid string, host string, f *StreamInfo) (error, []byte) {
	if f == nil {
		return errors.New("input f is null"), nil
	}

	b, err := json.Marshal(f)
	if err != nil {
		clr.logger.Errorf("logid:%v, json encode failed:%v.", logid, err)
		return err, nil
	}

	clr.logger.Infof("logid:%v, query file info from %v:%v\n", logid, host, string(b))
	body := bytes.NewBuffer([]byte(b))
	url := fmt.Sprintf("http://%v/inquire_stream2", host)
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.logger.Errorf("logid:%v, http post return failed.err:%v", logid, err)
		return err, nil
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.logger.Errorf("logid:%v, ioutil readall failed.err:%v", logid, err)
		return err, nil
	}
	return nil, result
}

func (clr *Cleaner) queryClusterByFileInfo(logid string, host string, f *FileInfo) (error, []byte) {
	if f == nil {
		return errors.New("input f is null"), nil
	}

	b, err := json.Marshal(f)
	if err != nil {
		clr.logger.Errorf("logid:%v, json encode failed:%v.", logid, err)
		return err, nil
	}

	clr.logger.Infof("logid:%v, query file info from %v:%v\n", logid, host, string(b))
	body := bytes.NewBuffer([]byte(b))
	url := fmt.Sprintf("http://%v/inquire_stream", host)
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.logger.Errorf("logid:%v, http post return failed.err:%v", logid, err)
		return err, nil
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.logger.Errorf("logid:%v, ioutil readall failed.err:%v", logid, err)
		return err, nil
	}
	return nil, result
}

func (clr *Cleaner) getRandomDeviceFromCluster(logid string, cls *devMgr.ClusterInfo) (*devMgr.DevInfo, int) {
	devs := clr.getDevsByCls(cls)
	dev_count := len(devs)
	if dev_count == 0 {
		clr.logger.Errorf("logid:%v, can't find devs to send clean order", logid)
		return nil, 0
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	idx := r.Intn(dev_count) % dev_count
	return &devs[idx], 1
}

//如果要删除的文件不在当前集群或者是在该集群删失败都返回2
func (clr *Cleaner) cleanFileFromCluster(logid string, cls *devMgr.ClusterInfo, f *FileInfo) (int, error) {
	//查询集群服务器获取输入详细信息
	err, b := clr.queryClusterByFileInfo(logid, cls.Host, f)
	if err != nil || b == nil {
		return 2, err
	}
	var m MediaInfoRet
	err = json.Unmarshal(b, &m)
	if err != nil {
		clr.logger.Errorf("logid:%v, queryClusterByFileInfo json decode failed:%v,b:%v", logid, err, string(b))
		return 2, nil
	}
	clr.logger.Infof("logid:%v, clean file:%+v", logid, m)
	if m.Errno == 2 || len(m.Data) == 0 {
		//不存在这个集群中
		return 2, nil
	}
	//todo 找一台该集群下的存储集群执行删除
	d, n := clr.getRandomDeviceFromCluster(logid, cls)
	if d == nil || n == 0 {
		return 2, nil
	}
	url := fmt.Sprintf("http://%s/post_delete", d.Host)
	clr.logger.Infof("logid:%v, clean task to %v", logid, url)
	r := clr.cleanEachDevice(url, m.Table, m.Data)
	if r == "" {
		clr.logger.Errorf("logid:%v, %v@%v clean task failed", logid, d.Name, d.Host)
		return 2, nil
	}
	clr.logger.Infof("logid:%v, clean device return:%v", logid, r)
	clr.ackCleanStatu2Cluster(d.Host, cls.Host, r)
	return 0, nil
}

//如果要删除的文件不在当前集群或者是在该集群删失败都返回2
func (clr *Cleaner) cleanStreamFileFromCluster(logid string, cls *devMgr.ClusterInfo, f *StreamInfo) (int, error) {
	//查询集群服务器获取输入详细信息
	err, b := clr.queryClusterByStreamInfo(logid, cls.Host, f)
	if err != nil || b == nil {
		return 2, err
	}
	var m []MediaInfoRet
	err = json.Unmarshal(b, &m)
	if err != nil {
		clr.logger.Errorf("logid:%v, queryClusterByStreamInfo json decode failed:%v,b:%v", logid, err, string(b))
		return 2, nil
	}
	clr.logger.Infof("logid:%v, clean file:%+v", logid, m)

	//todo 找一台该集群下的存储集群执行删除
	d, n := clr.getRandomDeviceFromCluster(logid, cls)
	if d == nil || n == 0 {
		return 2, nil
	}
	url := fmt.Sprintf("http://%s/post_delete", d.Host)
	clr.logger.Infof("logid:%v, clean task to %v", logid, url)

	for _, data := range m {
		if data.Errno == 0 {
			r := clr.cleanEachDevice(url, data.Table, data.Data)
			if r == "" {
				clr.logger.Errorf("logid:%v, %v@%v clean task failed", logid, d.Name, d.Host)
				return 2, nil
			}
			clr.logger.Infof("logid:%v, clean device return:%v", logid, r)
			clr.ackCleanStatu2Cluster(d.Host, cls.Host, r)
		}
	}
	
	return 0, nil
}

//单文件删除接口传入的参数
type FileInfo struct {
	Id        int    `json:"id"`
	Domain    string `json:"vod_host"`
	File      string `json:"file"`
	NotifyUrl string `json:"notify_url"`
}

//删除返回的信息结构
type DelFileRet struct {
	Errno  int    `json:"errno"`
	Errmsg string `json:"errmsg"`
	Id     int    `json:"id"`
	Domain string `json:"vod_host"`
	File   string `json:"file"`
}

type StreamInfo struct {
	Id        int           `json:"id"`
	Streams   []StreamItem  `json:"streams"`
	NotifyUrl string      `json:"notify_url"`
}

type StreamItem struct {
	Domain    string `json:"vod_host"`
	App      string `json:"app"`
	Stream string `json:"stream"`
	Date   string `json:"date"` // 格式为'Y-m-d'
}

//从给定的文件信息做删除操作
func (clr *Cleaner) CleanStreamFile(logid string, buf []byte) error {
	clr.logger.Infof("logid:%v, get req: %v", logid, string(buf))
	var ok bool
	var q StreamInfo
	err := json.Unmarshal(buf, &q)
	clr.logger.Errorf("logid:%v, q=%+v", logid, q)
	if err != nil {
		clr.logger.Errorf("logid:%v, Error: cannot decode req body %v", logid, err)
		goto RESPONSE
	}

	if len(q.NotifyUrl) == 0 || len(q.Streams) == 0 || (q.Id == 0) {
		clr.logger.Errorf("logid:%v, input error :domain:%v, file:%v, Id:%v",
			logid, q.NotifyUrl, q.Streams, q.Id)
		goto RESPONSE
	}

	//分别向所有集群下发删除指令
	for _, c := range clr.cls {
		ret, _ := clr.cleanStreamFileFromCluster(logid, &c, &q)
		if ret == 0 {
			ok = true
		}
	}

	//todo 回调请求携带的接口地址
	if ok == false {
		clr.logger.Errorf("logid:%v, clean %+v failed ", logid, q)
		goto RESPONSE
	}

	clr.logger.Infof("logid:%v, clean %+v ok ", logid, q)
RESPONSE:
	return nil
}

//从给定的文件信息做删除操作
func (clr *Cleaner) CleanFile(logid string, buf []byte) error {
	clr.logger.Infof("logid:%v, get req: %v", logid, string(buf))
	var ok bool
	var delRet DelFileRet
	var q FileInfo
	delRet.Errno = 2
	err := json.Unmarshal(buf, &q)
	if err != nil {
		clr.logger.Errorf("logid:%v, Error: cannot decode req body %v", logid, err)
		delRet.Errmsg = fmt.Sprintf("Error: cannot decode req body %v", err)
		goto RESPONSE
	}
	if len(q.Domain) == 0 || len(q.File) == 0 {
		clr.logger.Errorf("logid:%v, input error :domain:%v, file:%v", logid, q.Domain, q.File)
		delRet.Errmsg = fmt.Sprintf("input error :domain:%v, file:%v", q.Domain, q.File)
		goto RESPONSE
	}

	delRet.Id = q.Id
	delRet.Domain = q.Domain
	delRet.File = q.File
	//分别向所有集群下发删除指令
	for _, c := range clr.cls {
		ret, _ := clr.cleanFileFromCluster(logid, &c, &q)
		if ret == 0 {
			ok = true
		}
	}

	//todo 回调请求携带的接口地址
	if ok == false {
		clr.logger.Errorf("logid:%v, clean %+v failed ", logid, q)
		delRet.Errmsg = "failed"
		goto RESPONSE
	}
	delRet.Errno = 0
	delRet.Errmsg = "success"
	clr.logger.Infof("logid:%v, clean %+v ok ", logid, q)
RESPONSE:
	clr.ackForDelFiles(logid, q.NotifyUrl, &delRet)
	return nil
}

//删除成功之后通过该函数回调
func (clr *Cleaner) ackForDelFiles(logid string, host string, r *DelFileRet) (error, []byte) {
	b, err := json.Marshal(r)
	if err != nil {
		clr.logger.Errorf("logid:%v, json encode failed:%v.", logid, err)
		return err, nil
	}

	clr.logger.Infof("logid:%v, clean file ack to host:%v:%v\n", logid, host, string(b))
	body := bytes.NewBuffer([]byte(b))
	//url := fmt.Sprintf("http://%v/inquire_stream", host)
	url := host
	res, err := http.Post(url, "application/json;charset=utf-8", body)
	if err != nil {
		clr.logger.Errorf("logid:%v, http post return failed, err:%v", logid, err)
		return err, nil
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		clr.logger.Errorf("logid:%v, ioutil readall failed, err:%v", logid, err)
		return err, nil
	}
	clr.logger.Infof("logid:%v, response from %v:%v", logid, host, string(result))
	return nil, result
}
