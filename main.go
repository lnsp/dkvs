package main

import (
	"flag"
	"fmt"

	"github.com/lnsp/dkvs/nodes/local"
)

var (
	role       = flag.String("role", "master", "Set the node role (master, slave, client)")
	replicas   = flag.Int("replicas", 0, "Set the number of replicas stored")
	remoteHost = flag.String("remote", "localhost:5000", "Set the remote address")
	localHost  = flag.String("local", "localhost:5000", "Set the local address")
)

func main() {
	flag.Parse()
	var instance local.Node
	switch *role {
	case "master":
		instance = local.NewMaster(*localHost, *remoteHost, *replicas)
	case "slave":
		instance = local.NewSlave(*localHost, *remoteHost)
	default:
		fmt.Println("Mode", *role, "not supported")
	}
	instance.Listen()
}
