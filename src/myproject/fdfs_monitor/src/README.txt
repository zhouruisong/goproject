默认端口80，可以在main.go中的256行修改，执行go build main.go后才能生效

第一步：
tar -C /usr/local/ -zxvf go1.6.1.linux-amd64.tar.gz 
第二步：
vim /etc/profile
添加如下内容
export GOROOT=/usr/local/go
export PATH=$GOROOT/bin:$PATH
export GOPATH=/root/go-workspace

执行source /etc/profile

第三步：
当前目录下执行
./main &

