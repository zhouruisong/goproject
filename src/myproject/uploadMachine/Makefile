cluster_centre:
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go build -o cluster-centre src/main.go
	mv cluster-centre ./bin
.PHONY:clean
clean:
	rm ./bin/cluster-centre -vf
