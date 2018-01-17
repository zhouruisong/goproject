binlogsync:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	#go get gopkg.in/olivere/elastic.v3
	go build -o binlogsync src/main.go
	#go build -gcflags "-N -l" -o binlogsync src/main.go
	mv binlogsync ./bin
.PHONY:clean
clean:
	rm ./bin/binlogsync -vf
