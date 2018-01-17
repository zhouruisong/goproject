all:
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go get github.com/go-sql-driver/mysql
	go build -o cluster-centre cluster_centre/src/main.go
	go build -o tair-agent tair_agent/src/main.go
	go build -o mysql-agent mysql_agent/src/main.go
	go build -o fdfs-agent fdfs_agent/src/main.go
	mv cluster-centre tair-agent mysql-agent fdfs-agent ./bin
tair_agent:
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go build -o tair-agent tair_agent/src/main.go
	mv tair-agent ./bin
mysql_agent:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go build -o mysql-agent mysql_agent/src/main.go
	mv mysql-agent ./bin
fdfs_agent:
	go get github.com/go-sql-driver/mysql
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go build -o fdfs_agent fdfs_agent/src/main.go
	mv fdfs-agent ./bin
.PHONY:clean
clean:
	rm bin/cluster-centre -vf
	rm bin/tair-agent -vf
	rm bin/mysql-agent -vf
	rm bin/fdfs-agent -vf
