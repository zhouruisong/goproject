cluster_backup:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go get github.com/satori/go.uuid
	go build -o cluster_backup src/main.go
	#go build -gcflags "-N -l" -o cluster_backup src/main.go
	mv cluster_backup ./bin
.PHONY:clean
clean:
	rm ./bin/cluster_backup -vf
