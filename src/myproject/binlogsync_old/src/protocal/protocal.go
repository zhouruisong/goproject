package protocal

/////////////////////////////////////////////////////////
//mysql
/////////////////////////////////////////////////////////

// return of sending mysql data to other
type MsgInsertRet struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

// return of sending mysql data to other
type MsgMysqlRet struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

// upload machine send mysql data
type DbInfo struct {
	Id               int    `json:"id"`
	TaskId           string `json:"taskid"`
	FileName         string `json:"filename"`
	FileType         string `json:"filetype"`
	FileSize         int32  `json:"filesize"`
	Domain           string `json:"domain"`
	Status           int32  `json:"status"`
	Action           string `json:"action"`
	Md5Type          int16  `json:"md5type"`
	DnameMd5         string `json:"dnamemd5"`
	SourceUrl        string `json:"sourceurl"`
	TransCodingUrl   string `json:"transcodingurl"`
	FileMd5          string `json:"filemd5"`
	IndexMd5         string `json:"indexmd5"`
	HeadMd5          string `json:"headmd5"`
	ExpiryTime       string `json:"expirytime"`
	CreateTime       string `json:"createtime"`
	ExecTime         string `json:"exectime"`
	CbUrl            string `json:"cburl"`
	FfUri            string `json:"ffuri"`
	TaskBranchStatus string `json:"taskbranchstatus"`
	LocalServerDir   string `json:"localserverdir"`
	TsUrl            string `json:"tsurl"`
	Type             int8   `json:"type"`
	TransCodingInfo  string `json:"transcodinginfo"`
	IsBackup         int8   `json:"isbackup"`
}

// mysql db live_master table
type DbRowInfo struct {
	TaskId     string
	FileName   string
	FileType   string
	Domain     string
	Status     int32
	SourceUrl  string
	CbUrl      string
	CreateTime string
	FfUri      string
}

// send mysql data to other
type DbEventInfo struct {
	TableName string `json:"tablename"`
	DbData    DbInfo `json:"data"`
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
type IndexMap struct {
	Item []IndexInfo `json:"indexmap"`
}

// return of upload machine
type RetUploadMeg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// send to upload machine
type UploadInfo struct {
	TaskId    string `json:"taskid"`
	Domain    string `json:"domain"`
	FileName  string `json:"fname"`
	FileType  string `json:"ftype"`
	SourceUrl string `json:"url"`
	CbUrl     string `json:"cb_url"`
	Behavior  string `json:"behavior"`
	Md5Type   int16  `json:"md5_type"`
	IsBackup  int16  `json:"is_backup"`
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
	Filesize   int32  `json:"file_size"`
	CreateTime string `json:"create_time"`
	FfUri      string `json:"ff_uri"`
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
	FfUri      string `json:"ff_uri"`
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
