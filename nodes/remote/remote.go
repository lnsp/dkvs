package remote

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"log"

	"github.com/lnsp/dkvs/nodes"
)

const (
	maxReconnects     = 3
	reconnectInterval = time.Second
	cmdArgSeparator   = ";"
	cmdNameSeparator  = "#"
	StatusDown        = "DOWN"
	StatusReady       = "READY"
	StatusStartup     = "STARTUP"
	StatusShutdown    = "SHUTDOWN"
	ShutdownOK        = "OK"
	ShutdownDenied    = "DENIED"
	StoreOK           = "OK"
	StoreDenied       = "DENIED"
	RoleMasterPrimary = "PRIMARY"
	RoleMaster        = "SECONDARY"
	RoleSlave         = "GENERIC"
	JoinOK            = "OK"
	JoinDenied        = "DENIED"
	JoinInUse         = "IN USE"
	OwnOK             = "OK"
	OwnDenied         = "DENIED"
)

var (
	CommandCluster  = &Command{Name: "CLUSTER"}
	CommandReplicas = &Command{Name: "REPLICAS"}
	CommandRole     = &Command{Name: "ROLE"}
	CommandJoin     = &Command{Name: "JOIN"}
	CommandRevision = &Command{Name: "REVISION"}
	CommandStore    = &Command{Name: "STORE"}
	CommandRead     = &Command{Name: "READ"}
	CommandError    = &Command{Name: "ERROR"}
	CommandStatus   = &Command{Name: "STATUS"}
	CommandShutdown = &Command{Name: "SHUTDOWN"}
	CommandAddress  = &Command{Name: "ADDRESS"}
	CommandOwn      = &Command{Name: "OWN"}
)

type Node interface {
	Close()
	Queue(cmd *Command) (*Command, error)
	Poll() (*Command, error)
	Push(*Command) error
}

type Command struct {
	Name string
	Args []string
}

func (cmd Command) KindOf(kind *Command) bool {
	return cmd.Name == kind.Name
}

func (cmd Command) Arg(index int) string {
	if len(cmd.Args) <= index {
		return ""
	}
	return cmd.Args[index]
}

func (cmd Command) ArgCount() int {
	return len(cmd.Args)
}

func (cmd Command) Param(params ...string) *Command {
	return &Command{
		Name: cmd.Name,
		Args: params,
	}
}

func (cmd Command) Marshal() []byte {
	return []byte(cmd.Name + cmdNameSeparator + strings.Join(cmd.Args, cmdArgSeparator) + "\n")
}

func (cmd Command) String() string {
	return fmt.Sprintf("%s (%s)", cmd.Name, strings.Join(cmd.Args, ", "))
}

func UnmarshalCommand(cmd []byte) (*Command, error) {
	cmdString := string(cmd)
	cmdTokens := strings.Split(cmdString, cmdNameSeparator)
	if len(cmdTokens) < 1 {
		return nil, fmt.Errorf("Invalid command format: missing tokens")
	}
	name := cmdTokens[0]
	if len(cmdTokens) < 2 {
		return &Command{
			Name: name,
			Args: []string{},
		}, nil
	}
	args := cmdTokens[1]
	argTokens := strings.Split(args, cmdArgSeparator)
	return &Command{
		Name: name,
		Args: argTokens,
	}, nil
}

type Slave struct {
	Connection    net.Conn
	PublicAddress string
}

func (slave *Slave) remoteStatus() string {
	if slave.Connection != nil {
		response, err := slave.Queue(CommandStatus)
		if err != nil {
			return StatusDown
		}
		if !response.KindOf(CommandStatus) {
			return StatusDown
		}
		return response.Arg(0)
	}
	return StatusDown
}

func (slave *Slave) Push(cmd *Command) error {
	log.Println("Poll", cmd)
	_, err := slave.Connection.Write(cmd.Marshal())
	if err != nil {
		return errors.New("Could not write to socket")
	}
	return nil
}

func (slave *Slave) Poll() (*Command, error) {
	data, _, err := bufio.NewReader(slave.Connection).ReadLine()
	if err != nil {
		return nil, errors.New("Could not read from socket")
	}
	cmd, err := UnmarshalCommand(data)
	if err != nil {
		return nil, errors.New("Could not unmarshal command")
	}
	log.Println("Push", cmd)
	return cmd, nil
}

func (slave *Slave) Queue(cmd *Command) (*Command, error) {
	if err := slave.Push(cmd); err != nil {
		return nil, errors.New("Could not write request to socket")
	}
	respCmd, err := slave.Poll()
	if err != nil {
		return nil, errors.New("Failed to receive response")
	}
	if respCmd.KindOf(CommandError) {
		return nil, errors.New(respCmd.Arg(0))
	}
	if !respCmd.KindOf(cmd) {
		return nil, errors.New("Unexpected command response type")
	}
	return respCmd, nil
}

func (slave *Slave) keepConnectionAlive() error {
	if slave.remoteStatus() != StatusDown {
		return nil
	}
	conn, err := net.Dial("tcp", slave.PublicAddress)
	if err != nil {
		return err
	}
	if slave.Connection != nil {
		slave.Connection.Close()
	}

	slave.Connection = conn
	for i := 0; slave.remoteStatus() != StatusReady && i < maxReconnects; i++ {
		time.Sleep(reconnectInterval)
	}
	if slave.remoteStatus() != StatusReady {
		defer conn.Close()
		slave.Connection = nil
		return errors.New("Could not reach endpoint")
	}
	return nil
}

func (slave *Slave) Address() string {
	if err := slave.keepConnectionAlive(); err != nil {
		return slave.PublicAddress
	}
	response, err := slave.Queue(CommandAddress)
	if err != nil {
		return slave.PublicAddress
	}
	slave.PublicAddress = response.Arg(0)
	return slave.PublicAddress
}

func (slave *Slave) Read(key string) (string, nodes.Revision, error) {
	if err := slave.keepConnectionAlive(); err != nil {
		return key, nil, err
	}
	response, err := slave.Queue(CommandRead.Param(key))
	if err != nil {
		return key, nil, err
	}
	rev, err := nodes.ToRevision(response.Arg(1))
	if err != nil {
		return key, nil, err
	}
	return response.Arg(0), rev, nil
}

func (slave *Slave) Store(key, value string, rev nodes.Revision) error {
	if err := slave.keepConnectionAlive(); err != nil {
		return err
	}
	response, err := slave.Queue(CommandStore.Param(key, value, rev.String()))
	if err != nil {
		return err
	}
	if response.Arg(0) != StoreOK {
		return errors.New("Store denied by host")
	}
	return nil
}

func (slave *Slave) Status() nodes.Status {
	switch slave.remoteStatus() {
	case StatusDown:
		return nodes.StatusDown
	case StatusReady:
		return nodes.StatusReady
	case StatusStartup:
		return nodes.StatusStartup
	case StatusShutdown:
		return nodes.StatusShutdown
	default:
		return nodes.StatusDown
	}
}

func (slave *Slave) Shutdown() error {
	if err := slave.keepConnectionAlive(); err != nil {
		return err
	}
	response, err := slave.Queue(CommandShutdown)
	if err != nil {
		return err
	}
	if response.Arg(0) != ShutdownOK {
		return errors.New("Shutdown denied by host")
	}
	return nil
}

func (slave *Slave) Revision() (nodes.Revision, error) {
	if err := slave.keepConnectionAlive(); err != nil {
		return nil, err
	}
	response, err := slave.Queue(CommandRevision)
	if err != nil {
		return nil, err
	}
	bytes, err := nodes.ToRevision(response.Arg(0))
	if err != nil {
		return nil, err
	}
	return nodes.Revision(bytes), nil
}

func (slave *Slave) Own(m nodes.Master) error {
	if err := slave.keepConnectionAlive(); err != nil {
		return err
	}
	response, err := slave.Queue(CommandOwn.Param(m.Address()))
	if err != nil {
		return err
	}
	if response.Arg(0) != OwnOK {
		return errors.New("Own denied by host")
	}
	return nil
}

type Master struct {
	*Slave
}

func (master *Master) Cluster() ([]nodes.Node, error) {
	if err := master.keepConnectionAlive(); err != nil {
		return nil, err
	}
	response, err := master.Queue(CommandCluster)
	if err != nil {
		return nil, err
	}
	nodes := make([]nodes.Node, response.ArgCount())
	for i := 0; i < response.ArgCount(); i++ {
		nodes[i] = NewSlave(response.Arg(i))
	}
	return nodes, nil
}

func (master *Master) Replicas() ([]nodes.Master, error) {
	if err := master.keepConnectionAlive(); err != nil {
		return nil, err
	}
	response, err := master.Queue(CommandReplicas)
	if err != nil {
		return nil, err
	}
	replicas := make([]nodes.Master, response.ArgCount())
	for i := 0; i < response.ArgCount(); i++ {
		replicas[i] = NewMaster(response.Arg(i))
	}
	return replicas, nil
}

func (slave *Slave) Role() (nodes.Role, error) {
	if err := slave.keepConnectionAlive(); err != nil {
		return nodes.RoleSlave, err
	}
	response, err := slave.Queue(CommandRole)
	if err != nil {
		return nodes.RoleSlave, err
	}
	switch response.Arg(0) {
	case RoleMasterPrimary:
		return nodes.RoleMasterPrimary, nil
	case RoleMaster:
		return nodes.RoleMaster, nil
	default:
		return nodes.RoleSlave, nil
	}
}

func (slave *Slave) Close() {
	if slave.remoteStatus() != StatusDown {
		if slave.Connection != nil {
			slave.Connection.Close()
		}
		slave.Connection = nil
	}
}

func (master *Master) Join(n nodes.Node) error {
	if err := master.keepConnectionAlive(); err != nil {
		return err
	}
	response, err := master.Queue(CommandJoin.Param(n.Address()))
	if err != nil {
		return err
	}
	if response.Arg(0) != JoinOK {
		return errors.New("Join denied by host")
	}
	return nil
}

func NewMaster(addr string) *Master {
	return &Master{NewSlave(addr)}
}

func NewSlave(addr string) *Slave {
	return &Slave{PublicAddress: addr}
}
