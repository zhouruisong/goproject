package indexmgr

import (
	"../protocal"
	"bufio"
	"io"
	"os"
	"strings"
)

func getSliceData(line string, pMap *protocal.IndexMap) int {
	lineslice := strings.Split(line, " ")

	//	fmt.Println(lineslice)

	if len(lineslice) == 0 {
		return -1
	}

	data := protocal.IndexInfo{
		Name:   lineslice[0],
		Id:     lineslice[1],
		Status: lineslice[2],
		Size:   lineslice[3],
		Md5:    lineslice[4],
	}

	//	fmt.Println(data)

	pMap.Item = append(pMap.Item, data)

	return 0
}

func ReadLine(fileName string, pMap *protocal.IndexMap) error {
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return err
	}

	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			return nil
		}
		//		fmt.Println(line)
		rt := getSliceData(line, pMap)
		if rt != 0 {
			return nil
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

	return nil
}
