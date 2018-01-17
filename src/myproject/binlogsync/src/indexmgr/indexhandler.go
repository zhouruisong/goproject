package indexmgr

import (
//	"../protocal"
//	"fmt"
)

//var g_indexMap = make(map[string]protocal.IndexMap)
//
//func MapInsertItem(info *protocal.IndexInfo, taskid string) {
//	location := MapSearchItem(taskid)
//	if location == nil {
//		var indexMap protocal.IndexMap
//		indexMap.Item = append(indexMap.Item, *info)
//		g_indexMap[taskid] = indexMap
//	} else {
//		location.Item = append(location.Item, *info)
//		g_indexMap[taskid] = *location
//	}
//
//	// 遍历map
//	//	for k, v := range g_indexMap {
//	//		cl.Logger.Infof("k:%+v, v:%+v", k, v)
//	//	}
//
//	return
//}
//
//func MapSearchItem(taskid string) *protocal.IndexMap {
//	if v, ok := g_indexMap[taskid]; ok {
//		return &v
//	} else {
//		return nil
//	}
//}
//
//func MapDeleteItem(taskid string) bool {
//	delete(g_indexMap, taskid)
//
//	value, ok := g_indexMap[taskid]
//	if ok {
//		fmt.Println(value)
//		return false
//	} else {
//		fmt.Println("元素不存在")
//		return true
//	}
//}
