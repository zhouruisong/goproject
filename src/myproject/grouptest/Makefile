grouptest:
	go get github.com/go-martini/martini
	go get github.com/Sirupsen/logrus
	go build -o grouptest src/main.go
	mv grouptest ./bin
.PHONY:clean
clean:
	rm ./bin/grouptest -vf
