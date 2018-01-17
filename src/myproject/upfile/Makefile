upfile:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	#go get gopkg.in/olivere/elastic.v3
	go build -o upfile src/main.go
	#go build -gcflags "-N -l" -o binlogsync src/main.go
	mv upfile ./bin
.PHONY:clean
clean:
	rm ./bin/upfile -vf
