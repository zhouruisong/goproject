package main

import (
    "context"
	"fmt"
	"github.com/smallnest/rpcx/client"
	"time"
)

type Args struct {
	A int `msg:"a"`
	B int `msg:"b"`
}
type Reply struct {
	C int `msg:"c"`
}

func main() {
	discovery: = client.ServiceDiscovery{}
	s := client.NewOneClient(2,1,)
	c := client.NewOneClient(s)
	args := &Args{7, 8}
	var reply Reply
	err := c.Call(context.Background(),"Arith", "Mul", args, &reply)
	if err != nil {
		fmt.Printf("error for Arith: %d*%d, %v \n", args.A, args.B, err)
	} else {
		fmt.Printf("Arith: %d*%d=%d \n", args.A, args.B, reply.C)
	}
	c.Close()
}
