rpc_server:
	#go get github.com/go-martini/martini
	#go get -tags "reuseport kcp zookeeper" github.com/smallnest/rpcx/...
	#go build -tags "reuseport kcp zookeeper etcd consul ping utp rudp" github.com/smallnest/rpcx/...
	#go get github.com/smallnest/rpcx/...
	#go get -u -v -tags "zookeeper" github.com/smallnest/rpcx/...go build -o rpc_test src/main.go
	go build -o rpc_server src/main.go
	mv rpc_server ./bin
.PHONY:clean
clean:
	rm ./bin/rpc_server -vf
