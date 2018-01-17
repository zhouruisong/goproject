package binlogmgr

import (
	"time"
)

func reflectint8(ty interface{}) (int8, bool) {
	if value, ok := ty.(int8); ok {
		return value, true
	}

	return 0, false
}

func reflectint16(ty interface{}) (int16, bool) {
	if value, ok := ty.(int16); ok {
		return value, true
	}

	return 0, false
}

func reflectint32(ty interface{}) (int32, bool) {
	if value, ok := ty.(int32); ok {
		return value, true
	}

	return 0, false
}

func reflectint64(ty interface{}) (int64, bool) {
	if value, ok := ty.(int64); ok {
		return value, true
	}

	return 0, false
}

func reflectString(ty interface{}) (string, bool) {
	if value, ok := ty.(string); ok {
		return value, true
	}

	return "", false
}

func reflectTime(ty interface{}) (string, bool) {
	if value, ok := ty.(time.Time); ok {
		return value.Format("2006-01-02 15:04:05"), true
	}

	return "", false
}

/*
func GetValue(data *protocal.DbInfo, k int, d interface{}) {
	switch k {
	case 1:
		data.TaskId, _ = reflectString(d)
	case 2:
		data.FileName, _ = reflectString(d)
	case 3:
		data.FileType, _ = reflectString(d)
	case 4:
		data.FileSize, _ = reflectint32(d)
	case 5:
		data.Domain, _ = reflectString(d)
	case 6:
		data.Status, _ = reflectint32(d)
	case 7:
		data.Action, _ = reflectString(d)
	case 8:
		data.Md5Type, _ = reflectint16(d)
	case 9:
		data.DnameMd5, _ = reflectString(d)
	case 10:
		data.SourceUrl, _ = reflectString(d)
	case 11:
		data.TransCodingUrl, _ = reflectString(d)
	case 12:
		data.FileMd5, _ = reflectString(d)
	case 13:
		data.IndexMd5, _ = reflectString(d)
	case 14:
		data.HeadMd5, _ = reflectString(d)
	case 15:
		data.ExpiryTime, _ = reflectString(d)
	case 16:
		data.CreateTime, _ = reflectString(d)
	case 17:
		data.ExecTime, _ = reflectString(d)
	case 18:
		data.CbUrl, _ = reflectString(d)
	case 19:
		data.FfUri, _ = reflectString(d)
	case 20:
		data.TaskBranchStatus, _ = reflectString(d)
	case 21:
		data.LocalServerDir, _ = reflectString(d)
	case 22:
		data.TsUrl, _ = reflectString(d)
	case 23:
		data.Type, _ = reflectint8(d)
	case 24:
		data.TransCodingInfo, _ = reflectString(d)
	case 25:
		data.IsBackup, _ = reflectint8(d)
	default:
	}
}*/
