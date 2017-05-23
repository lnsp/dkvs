package main

import "flag"

var (
	role     = flag.String("role", "master", "Set the node role (master, slave, client)")
	replicas = flag.Int("replicas", 0, "Set the number of replicas stored")
	remote   = flag.String("remote", "localhost", "Set the remote address")
)

func main() {

}
