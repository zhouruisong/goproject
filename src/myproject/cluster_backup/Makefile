cluster-test:
	go get github.com/Sirupsen/logrus
	go build -o cluster-test src/main.go
	mv cluster-test ./bin
.PHONY:clean
clean:
	rm ./bin/cluster-test -vf
