package indexmgr

import (
	"../protocal"
	//	"fmt"
	"strings"
)

func getSliceData(line string, indexcache *protocal.IndexCache) {
	lineslice := strings.Split(line, " ")
	//	fmt.Println(lineslice)

	if len(lineslice) == 1 {
		// 最后一行为空，跳过
		if lineslice[0] == "" {
			//			fmt.Println("===")
			return
		}

		// 获取文件总大小
		indexcache.FileSize = lineslice[0]
		//		fmt.Println(indexcache.FileSize)
	} else {

		data := protocal.IndexInfo{
			Name:   lineslice[0],
			Id:     lineslice[1],
			Status: lineslice[2],
			Size:   lineslice[3],
			Md5:    lineslice[4],
		}

		indexcache.Item = append(indexcache.Item, data)
	}

	return
}

/* 二级索引格式如下：
52428800
my15286.mp4_0 group1/M02/00/0F/wKhuHlka4U6AQNtVAKAAAHBWCyc8.mp4_0 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_1 group1/M03/00/0F/wKhuHlka4U6AfX2rAKAAAHBWCyc3.mp4_1 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_2 group1/M04/00/0F/wKhuHlka4U6AEkY7AKAAAHBWCyc8.mp4_2 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_3 group1/M05/00/0F/wKhuHlka4U6AbFFWAKAAAHBWCyc9.mp4_3 0 10485760 dff93aed140e1b0584ba6aabfb491061
my15286.mp4_4 group1/M00/00/0F/wKhuHlka4U6ANyjJAKAAAHBWCyc4.mp4_4 0 10485760 dff93aed140e1b0584ba6aabfb491061
*/

func ReadLine(inputBuf []byte, indexcache *protocal.IndexCache) error {
	//	fmt.Print("\ninputBuf:\n", string(inputBuf))

	buff := string(inputBuf)
	linebuf := strings.Split(buff, "\n")

	length := len(linebuf)
	//	fmt.Println(length)
	for i := 0; i < length; i++ {
		//		fmt.Println(i, linebuf[i])
		//		fmt.Print(linebuf[i])
		getSliceData(linebuf[i], indexcache)
	}

	//	fmt.Print(indexcache)
	return nil
}
