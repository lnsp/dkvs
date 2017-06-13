// Command dkvsd starts a new cluster node.
//
// Usage
//    -local string
//          set the local address (default "localhost:5000")
//    -remote string
//          set the remote address (default "localhost:5000")
//    -replicas int
//          number of replicas in cluster (default 1)
//    -role string
//          set node role to either master, slave or client (default "master")
//    -v    display version of dkvs
//
// Examples:
//    # start a master on the local machine.
//    dkvsd -role master -replicas 3 -remote ""
//    # start a node to join the cluster
//    dkvsd -role slave -remote localhost:5000 -local localhost:5001
//
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lnsp/dkvs/lib/local"
)

const (
	versionText = "version:\n\tdkvsd-0.0.2-indev"
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
