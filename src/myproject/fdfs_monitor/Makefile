fdfs_monitor:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go get github.com/satori/go.uuid
	go get github.com/2tvenom/myreplication
	#go build -o cluster_backup src/main.go
	go build -gcflags "-N -l" -o fdfs_monitor src/main.go
	mv fdfs_monitor ./bin
.PHONY:clean
clean:
	rm ./bin/fdfs_monitor -vf
