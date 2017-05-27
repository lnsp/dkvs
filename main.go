package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lnsp/dkvs/nodes/local"
)

const (
	versionText = "version:\n\tdkvs-0.0.1-indev"
)

func main() {
	role := flag.String("role", "master", "set node role to either master, slave or client")
	replicas := flag.Int("replicas", 1, "number of replicas in cluster")
	remoteHost := flag.String("remote", "localhost:5000", "set the remote address")
	localHost := flag.String("local", "localhost:5000", "set the local address")
	version := flag.Bool("v", false, "display version of dkvs")
	flag.Parse()

	if *version {
		fmt.Println(versionText)
		os.Exit(0)
	}

	var instance local.Node
	switch *role {
	case "master":
		instance = local.NewMaster(*localHost, *remoteHost, *replicas)
	case "slave":
		instance = local.NewSlave(*localHost, *remoteHost)
	default:
		fmt.Printf("usage:\n\t%s [-v] [--role [master | slave | client]] [--replicas [N]] [--local ...] [--remote ...]\n", os.Args[0])
		os.Exit(1)
	}
	instance.Listen()
}
