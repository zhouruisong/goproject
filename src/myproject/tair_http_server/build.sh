cur_path=`pwd`
export GOPATH=$cur_path:$GOPATH
go get github.com/go-martini/martini
go get github.com/Sirupsen/logrus
go build -o bin/gotair src/main.go
