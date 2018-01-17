package static

import (
	"fmt"
	"net/http"
	"os"
	//"io"
	"io/ioutil"
	"io"
	"strconv"
	"strings"
	"os/exec"
	"path/filepath"
)

var url  = "http://s3s.lecloud.com/Document/VolumeData/"
var area_map = map[string]string{
	"19":"北京亦庄2",
	"21":"北京亦庄3",
	"207":"宁波电信2",
	"209":"广州电信2",
	"213":"广州电信3",
	"308":"廊坊联通",
	"210":"西安电信",
	"309":"太原联通2",
	"211":"临安电信",
	"310":"沈阳联通2",
	"208":"南京电信2",
	"902":"香港和记",
	"903":"香港PCCW",
	"911":"美国华盛顿",
	"912":"美国洛杉矶",
	"920":"巴黎电讯"}



func readFile(filepath string)([]byte,error)  {
	f,err := os.Open(filepath)
	if err != nil {
		return nil,err
	}

	return ioutil.ReadAll(f)
}

func Handler(w http.ResponseWriter, r *http.Request)  {
	st := ""
	et := ""
	r.ParseForm()
	if len(r.Form["st"]) <= 0 || len(r.Form["et"]) <= 0 {
		fmt.Fprintf(w,"no st or et!")
		return
	}
	st = r.Form["st"][0]
	et = r.Form["et"][0]
	cst := 0
	cet := 0
	result := "{"
	for nodeid,nodename := range area_map{

		result = result + "\"" + nodeid + "\"" +": "+ "{\"nodeName\": " + "\"" + nodename + "\"" + ",\"resultSet\": ["
		start ,err := strconv.Atoi(st)
		if err != nil {
			fmt.Print(nodename)
			panic(err)
		}
		end , err := strconv.Atoi(et)
		for i:=start; i<=end; i=i+86400000 {
			cst = i-86400000
			cet = i;
			csts  := strconv.Itoa(cst)
			cets  := strconv.Itoa(cet)
			filename := "calc_"+nodeid+"-"+csts+"-"+cets

			// get location
			file, _ := exec.LookPath(os.Args[0])
			pathabs, _ := filepath.Abs(file)
			index := strings.LastIndex(pathabs, string(os.PathSeparator))
			ret := pathabs[:index]

			path := fmt.Sprintf("%s/%s/%s", ret, "static", "VolumeData")
			filename_read := path + "/" + filename

			f, err := os.Open(filename_read)
			//没有该文件则下载文件
			if err != nil && os.IsNotExist(err){
				resp,err := http.Get(url+filename)
				if err != nil || resp.StatusCode!=200{
					//panic(err)
					// fmt.Print(path)
					fmt.Fprintf(w,"Cannot find file: " + url + filename + "\n后续会解决^-^")
					return
				}
				//在当前目录下生成VolumeData目录
				err = os.Mkdir(path, os.ModePerm)
				if err != nil {
				}
				//创建文件
				df,err := os.Create(filename_read)
				if err != nil {
					// fmt.Print(path)
					fmt.Fprintf(w,"Cannot create file: " + path + "\n后续会解决^-^")
					return
				}
				io.Copy(df,resp.Body)
			}
			f.Close()
			result_byte ,err:= readFile(filename_read)
			result_str := string(result_byte[:])
			if i+86400000 <= end  {
				result = result + result_str + ","
			} else {
				result = result + result_str + "]},"
			}
		}
	}
	result = result[0:len(result)-1]
	result = result + "}"
	result = strings.Replace(result, "\n", "", -1)
	fmt.Fprintf(w,result)
}

// func main() {
// 	http.HandleFunc("/volumeData",handler)
// 	http.Handle("/",http.FileServer(http.Dir("./")))
// 	http.ListenAndServe("10.75.145.178:8085",nil)
// }
