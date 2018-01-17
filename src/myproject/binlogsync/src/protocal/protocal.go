package protocal

/////////////////////////////////////////////////////////
//mysql
/////////////////////////////////////////////////////////

// return of sending mysql data to other
type MsgMysqlRet struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

// upload machine send mysql body
type MsgMysqlBody struct {
	TableName string `json:"tablename"`
	Data      DbInfo `json:"data"`
}

// upload machine send mysql data
type DbInfo struct {
	ID         int    `json:"id"`
	TaskID     string `json:"task_id"`
	SubtaskID  string `json:"subtask_id"`
	FileName   string `json:"file_name"`
	FileType   string `json:"file_type"`
	FileSize   int    `json:"file_size"`
	Domain     string `json:"domain"`
	App        string `json:"app"`
	SourceType string `json:"source_type"`
	URI        string `json:"uri"`
	CbURL      string `json:"cb_url"`
	FileMd5    string `json:"file_md5"`
	IndexMd5   string `json:"index_md5"`
	HeadMd5    string `json:"head_md5"`
	ExpiryTime string `json:"expiry_time"`
	CreateTime string `json:"create_time"`
	IsBackup   int    `json:"is_backup"`
}

// second index
type IndexInfo struct {
	Name   string `json:"name"`
	Id     string `json:"id"`
	Status string `json:"status"`
	Size   string `json:"size"`
	Md5    string `json:"md5"`
}

// second index in memory
type IndexCache struct {
	FileSize string      `json:"filesize"`
	Item     []IndexInfo `json:"indexmap"`
}

// return of upload machine
type RetUploadMeg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// send to upload machine
type UploadInfo struct {
	TaskId   string `json:"taskid"`
	Domain   string `json:"domain"`
	FileName string `json:"fname"`
	FileType string `json:"ftype"`
	Url      string `json:"url"`
	CbUrl    string `json:"cb_url"`
	Behavior string `json:"behavior"`
	Md5Type  int16  `json:"md5_type"`
	IsBackup int    `json:"is_backup"`
}

// send to upload machine
type IndexTaskInfo struct {
	TaskId    string
	IndexName string
}

/////////////////////////////////////////////////////////
//fastdfs
/////////////////////////////////////////////////////////
// 向fastdfs存储数据请求接口
type CentreUploadFile struct {
	Taskid  string `json:"taskid"`
	Content []byte `json:"content"`
}

// 向fastdfs存储数据返回接口
type RetCentreUploadFile struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
	Id     string `json:"id"`
}

// 向fastdfs下载数据请求接口
type CentreDownloadFile struct {
	Id string `json:"id"`
}

// 向fastdfs下载数据返回接口
type RetCentreDownloadFile struct {
	Errno   int    `json:"code"`
	Errmsg  string `json:"message"`
	Content []byte `json:"content"`
}

// 向fastdfs下载数据请求接口
type CentreDeleteFile struct {
	Id string `json:"id"`
}

// 向fastdfs存储数据返回接口
type RetCentreDeleteFile struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

/////////////////////////////////////////////////////////
//tair
/////////////////////////////////////////////////////////

//get inferface
////////////////////////////////////////////////////////
type SendTairGet struct {
	Prefix string `json:"prefix"`
	Key    string `json:"key"`
}
type SednTairGetBody struct {
	Keys []SendTairGet `json:"keys"`
}
type SendTairMesageGet struct {
	Command    string        `json:"command"`
	ServerAddr string        `json:"server_addr"`
	GroupName  string        `json:"group_name"`
	Keys       []SendTairGet `json:"keys"`
}
type RetTairGetDetail struct {
	Prefix     string `json:"prefix"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	CreateTime string `json:"createtime"`
	ExpireTime string `json:"expiretime"`
}
type RetTairGet struct {
	Errno  int                `json:"code"`
	Errmsg string             `json:"message"`
	Keys   []RetTairGetDetail `json:"keys"`
}
type RetTairGetKeys struct {
	Keys []RetTairGetDetail `json:"keys"`
}

type MsgTairRet struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

//put interface
////////////////////////////////////////////////////////
type SendTairPut struct {
	Prefix     string `json:"prefix"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	CreateTime string `json:"createtime"`
	ExpireTime string `json:"expiretime"`
}
type SednTairPutBody struct {
	Keys []SendTairPut `json:"keys"`
}
type SendTairMesage struct {
	Command    string        `json:"command"`
	ServerAddr string        `json:"server_addr"`
	GroupName  string        `json:"group_name"`
	Keys       []SendTairPut `json:"keys"`
}
type RetTairPut struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

/////////////////////////////////////////////////////////
//elasticsearch
/////////////////////////////////////////////////////////
type SendEsBody struct {
	TaskId     string `json:"task_id"`
	Action     int32  `json:"action"`
	Domain     string `json:"domain"`
	FileName   string `json:"filename"`
	Filesize   int    `json:"file_size"`
	CreateTime string `json:"create_time"`
	Uri        string `json:"uri"`
}

//查询结果返回结构
type GetEsInput struct {
	Domain   string `json:"domain"`
	FileName string `json:"filename"`
}

type RetGetEsItems struct {
	TaskId     string `json:"task_id"`
	Action     int32  `json:"action"`
	Domain     string `json:"domain"`
	FileName   string `json:"filename"`
	Filesize   int32  `json:"file_size"`
	CreateTime string `json:"create_time"`
	Uri        string `json:"uri"`
}

type DataInfos struct {
	Index        string        `json:"_index"`
	Type         string        `json:"_type"`
	Id           string        `json:"_id"`
	Score        float32       `json:"_score"`
	RetGetEsItem RetGetEsItems `json:"_source"`
}

type Hits struct {
	Total     int         `json:"total"`
	MaxScore  float32     `json:"max_score"`
	DataInfos []DataInfos `json:"hits"`
}

type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type RetGetEsBody struct {
	Took    int    `json:"took"`
	TimeOut bool   `json:"timed_out"`
	Shard   Shards `json:"_shards"`
	Hit     Hits   `json:"hits"`
}

//查询结果返回结构

//查询文件是否存在返回结构
type RetCheckExist struct {
	Exists bool `json:"exists"`
}

type EsIndexStruct struct {
	Took    int    `json:"took"`
	TimeOut bool   `json:"timed_out"`
	Shard   Shards `json:"_shards"`
	Hit     Hits   `json:"hits"`
}

type Ctx struct {
	Number   int
	Length   int
	Cache    *IndexCache
	Data     *MsgMysqlBody
	ResIndex []int
	ResCh    chan int
}
