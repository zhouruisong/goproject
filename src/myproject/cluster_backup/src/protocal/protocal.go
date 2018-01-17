package protocal

type TimeInfo struct
{
	hour int
	minute int
	second int
}

type CheckInfo struct {
	Tablename string
	Info      StreamInfo
}

type InsertInfo struct {
	TaskId       string
	TaskServer   string
	FileName     string
	FileType     int8
	FileSize     int32
	FileMd5      string
	Domain       string
	App          string
	Stream       string
	Step         int8
	PublishTime  int64
	NotifyUrl    string
	NotifyReturn string
	Status       int8
	ExpireTime   string
	CreateTime   string
	UpdateTime   string
	EndTime      string
	NotifyTime   string
}

// mysql db live_master table 
type StreamInfo struct {
	Id           int32
	TaskId       string
	TaskServer   string
	FileName     string
	FileType     int8
	FileSize     int32
	FileMd5      string
	Domain       string
	App          string
	Stream       string
	Step         int8
	PublishTime  int64
	NotifyUrl    string
	NotifyReturn string
	Status       int8
	ExpireTime   string
	CreateTime   string
	UpdateTime   string
	EndTime      string
	NotifyTime   string
}

// 向fastdfs存储数据请求接口
type CentreUploadFile struct {
	Filename string     `json:"filename"`
	Content  []byte     `json:"content"`
}

// 向fastdfs存储数据请求接口(对外不公开)
type CentreUploadFileEx struct {
	Logid    string     `json:"logid"`
	Filename string     `json:"filename"`
	Content  []byte     `json:"content"`
}

// 向fastdfs存储数据返回接口
type RetCentreUploadFile struct {
	Errno  int        `json:"code"`
	Errmsg string     `json:"message"`
	Id     string     `json:"id"`
}

// 向fastdfs下载数据请求接口
type CentreDownloadFile struct {
	Id     string     `json:"id"`
}

// 向fastdfs下载数据请求接口
type CentreDownloadFileEx struct {
	Logid    string     `json:"logid"`
	Id     string     `json:"id"`
}

// 向fastdfs下载数据返回接口
type RetCentreDownloadFile struct {
	Errno  int        `json:"code"`
	Errmsg string     `json:"message"`
	Content  []byte   `json:"content"`
}

/////////////////////////////////////////////////////////
type SendTairGet struct {
    Prefix string `json:"prefix"`
    Key    string `json:"key"`
}
type SednTairGetBody struct {
	Keys       []SendTairGet `json:"keys"`
}
type SednTairGetBodyEX struct {
	Logid    string     `json:"logid"`
	Message  SednTairGetBody  `json:"message"`
}
type SendTairMesageGet struct {
	Command    string      `json:"command"`
	ServerAddr string      `json:"server_addr"`
	GroupName  string      `json:"group_name"`
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
	Errno  int        `json:"code"`
	Errmsg string     `json:"message"`
	Keys []RetTairGetDetail `json:"keys"`
}
type RetTairGetKeys struct {
	Keys []RetTairGetDetail `json:"keys"`
}

////////////////////////////////////////////////////////
type SendTairPut struct {
    Prefix     string `json:"prefix"`
    Key        string `json:"key"`
    Value      string `json:"value"`
    CreateTime uint64 `json:"createtime"`
    ExpireTime uint64 `json:"expiretime"`
}
type SednTairPutBody struct {
	Keys       []SendTairPut `json:"keys"`
}
type SednTairPutBodyEX struct {
	Logid    string     `json:"logid"`
	Message  SednTairPutBody  `json:"message"`
}	
type SendTairMesage struct {
	Command    string      `json:"command"`
	ServerAddr string      `json:"server_addr"`
	GroupName  string      `json:"group_name"`
	Keys       []SendTairPut `json:"keys"`
}
type RetTairPut struct {
	Errno  int        `json:"code"`
	Errmsg string     `json:"message"`
}