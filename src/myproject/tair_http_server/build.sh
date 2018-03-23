cur_path=`pwd`
export GOPATH=$cur_path:$GOPATH
go install github.com/go-martini/martini
go install github.com/Sirupsen/logrus
go build -o bin/tairclient src/main.go
