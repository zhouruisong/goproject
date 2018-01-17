storage_center:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go get github.com/satori/go.uuid
	go build -o storage_center src/main.go
	mv storage_center ./bin
.PHONY:clean
clean:
	rm ./bin/storage_center -vf
