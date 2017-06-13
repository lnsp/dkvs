// Package remote provides a remote interface for network nodes.
package remote

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"bufio"

	"github.com/lnsp/dkvs/lib"
)

// A node in the cluster can either be down (not reachable), ready (usable), in startup mode (online, but not yet usable)
// or in shutdown mode (online, soon unreachable).
const (
	StatusDown     = "DOWN"
	StatusReady    = "READY"
	StatusStartup  = "STARTUP"
	StatusShutdown = "SHUTDOWN"
)

// A nodes role in the cluster can either be primary. secondary or generic.
const (
	RoleMasterPrimary = "PRIMARY"
	RoleMaster        = "SECONDARY"
	RoleSlave         = "GENERIC"
)

// Most command respond with a binary state, most often OK or DENIED.
const (
	ShutdownOK     = "OK"
	ShutdownDenied = "DENIED"
	StoreOK        = "OK"
	StoreDenied    = "DENIED"
	JoinOK         = "OK"
	JoinDenied     = "DENIED"
	JoinInUse      = "IN USE"
	RebuildOK      = "OK"
	HandshakeOK    = "OK"
	AssistOK       = "OK"
	RevisionOK     = "OK"
	MirrorOK       = "OK"
)

const (
	maxReconnects     = 5
	reconnectInterval = 3 * time.Second
	cmdArgSeparator   = ";"
	cmdNameSeparator  = "#"
)

var (
	// CommandCluster retrieves a list of nodes in the cluster.
	CommandCluster = &Command{Name: "CLUSTER"}
	// CommandReplicas retrieves a list of masters in the cluster.
	CommandReplicas = &Command{Name: "REPLICAS"}
	// CommandRole sets and gets the nodes role.
	CommandRole = &Command{Name: "ROLE"}
	// CommandJoin tells the master that a specific node wants to join its cluster.
	CommandJoin = &Command{Name: "JOIN"}
	// CommandRevision gets the nodes local revision.
	CommandRevision = &Command{Name: "REVISION"}
	// CommandStore tells the cluster to store a key-value pair.
	CommandStore = &Command{Name: "STORE"}
	// CommandLocalStore tells the node to store a key-value pair locally.
	CommandLocalStore = &Command{Name: "STORE_LOCAL"}
	// CommandLocalRead retrieves a local key-value pair from the node.
	CommandLocalRead = &Command{Name: "READ_LOCAL"}
	// CommandRead retrieves a key-value pair from the cluster.
	CommandRead = &Command{Name: "READ"}
	// CommandError signals an error.
	CommandError = &Command{Name: "ERROR"}
	// CommandStatus sets and gets the nodes internal status.
	CommandStatus = &Command{Name: "STATUS"}
	// CommandShutdown kills the cluster node.
	CommandShutdown = &Command{Name: "SHUTDOWN"}
	// CommandAddress retrieves the nodes public address.
	CommandAddress = &Command{Name: "ADDRESS"}
	// CommandRebuild tells the node to rebuild its cluster access data.
	CommandRebuild = &Command{Name: "REBUILD"}
	// CommandHandshake does nothing than shakin dem hands.
	CommandHandshake = &Command{Name: "HANDSHAKE"}
	// CommandAssist tells the master that a node wants to assist in the cluster as a master.
	CommandAssist = &Command{Name: "ASSIST"}
	// CommandMirror tells the receiver to mirror the specified node.
	CommandMirror = &Command{Name: "MIRROR"}
	// CommandLocalKeys retrieves the list of locally stored keys.
	CommandLocalKeys = &Command{Name: "KEYS_LOCAL"}
	// CommandKeys retrieves the list of keys stored in the cluster.
	CommandKeys = &Command{Name: "KEYS"}
)

// Node is a remote node connection.
type Node interface {
	Close()
	Queue(cmd *Command) (*Command, error)
	Poll() (*Command, error)
	Push(*Command) error
}

// Error generates a parameterized error command.
func Error(err error) *Command {
	return CommandError.Param(strings.ToUpper(err.Error()))
}

// Command is a generic DKVS command that can be send via a network connection.
type Command struct {
	Name string
	Args []string
}

// KindOf checks if the command types are the same.
func (cmd Command) KindOf(kind *Command) bool {
	return cmd.Name == kind.Name
}

// Arg gets the command argument at index i.
func (cmd Command) Arg(index int) string {
	if len(cmd.Args) <= index {
		return ""
	}
	return cmd.Args[index]
}

// ArgCount returns the number of arguments.
func (cmd Command) ArgCount() int {
	return len(cmd.Args)
}

// ArgList returns a copy of the commands arguments.
func (cmd Command) ArgList() []string {
	return cmd.Args[:]
}

// Param builds a new command instance with the same type but different arguments.
func (cmd Command) Param(params ...string) *Command {
	return &Command{
		Name: cmd.Name,
		Args: params,
	}
}

// Marshal converts the command into a slice of bytes.
func (cmd Command) Marshal() []byte {
	return []byte(cmd.Name + cmdNameSeparator + strings.Join(cmd.Args, cmdArgSeparator) + "\n")
}

// String outputs the command in a human readable format.
func (cmd Command) String() string {
	return fmt.Sprintf("%s (%s)", cmd.Name, strings.Join(cmd.Args, ", "))
}

// UnmarshalCommand generates a command from a string of bytes. It may return an error while parsing.
func UnmarshalCommand(cmd []byte) (*Command, error) {
	cmdString := string(cmd)
	cmdTokens := strings.Split(cmdString, cmdNameSeparator)
	if len(cmdTokens) < 1 {
		return nil, fmt.Errorf("Invalid command format: missing tokens")
	}
	name := strings.ToUpper(cmdTokens[0])
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

// Slave is a remotely connected cluster slave.
type Slave struct {
	Connection    net.Conn
	PublicAddress string
	Dead          bool
	reader        *bufio.Reader
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
	if !slave.Dead {
		err := slave.Open()
		if err != nil {
			return err
		}
	} else {
		return errors.New("Could not push to dead connection")
	}
	slave.Log("push", cmd)
	_, err := slave.Connection.Write(cmd.Marshal())
	if err != nil {
		slave.Reset()
		return errors.New("Could not write to socket")
	}
	return nil
}

func (slave *Slave) Log(tag string, args ...interface{}) {
	fmt.Print("["+strings.ToUpper(tag)+"] ", fmt.Sprintln(args...))
}

func (slave *Slave) Reset() {
	if slave.Connection != nil {
		slave.Connection.Close()
		slave.Connection = nil
	}
}

func (slave *Slave) Poll() (*Command, error) {
	if !slave.Dead {
		err := slave.Open()
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Could not read from dead connection")
	}
	if slave.reader == nil {
		slave.reader = bufio.NewReader(slave.Connection)
	}
	data, _, err := slave.reader.ReadLine()
	if err != nil {
		slave.Reset()
		return nil, errors.New("Could not read from socket")
	}
	cmd, err := UnmarshalCommand(data)
	if err != nil {
		return nil, errors.New("Could not unmarshal command")
	}
	slave.Log("poll", cmd)
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

func (slave *Slave) Open() error {
	if slave.Connection != nil {
		return nil
	}
	conn, err := net.Dial("tcp", slave.PublicAddress)
	if err != nil {
		return err
	}
	slave.Connection = conn
	slave.reader = bufio.NewReader(slave.Connection)
	for i := 0; slave.remoteStatus() != StatusReady; i++ {
		time.Sleep(reconnectInterval)
		if i >= maxReconnects {
			slave.Connection.Close()
			slave.Connection = nil
			return errors.New("Could not reach endpoint")
		}
	}
	return nil
}

func (slave *Slave) Address() string {
	if slave.PublicAddress != "" {
		return slave.PublicAddress
	}
	response, err := slave.Queue(CommandAddress)
	if err != nil {
		return slave.PublicAddress
	}
	slave.PublicAddress = response.Arg(0)
	return slave.PublicAddress
}

func (slave *Slave) Read(key string) (string, lib.Revision, error) {
	response, err := slave.Queue(CommandRead.Param(key))
	if err != nil {
		return key, nil, err
	}
	rev, err := lib.ToRevision(response.Arg(1))
	if err != nil {
		return key, nil, err
	}
	return response.Arg(0), rev, nil
}

func (slave *Slave) LocalRead(key string) (string, lib.Revision, error) {
	response, err := slave.Queue(CommandLocalRead.Param(key))
	if err != nil {
		return key, nil, err
	}
	rev, err := lib.ToRevision(response.Arg(1))
	if err != nil {
		return key, nil, err
	}
	return response.Arg(0), rev, nil
}

func (slave *Slave) Store(key, value string, rev lib.Revision) error {
	revString := ""
	if rev != nil {
		revString = rev.String()
	}

	response, err := slave.Queue(CommandStore.Param(key, value, revString))
	if err != nil {
		return err
	}
	if response.Arg(0) != StoreOK {
		return errors.New("Store denied by host")
	}
	return nil
}

func (slave *Slave) LocalStore(key, value string, rev lib.Revision) error {
	response, err := slave.Queue(CommandLocalStore.Param(key, value, rev.String()))
	if err != nil {
		return err
	}
	if response.Arg(0) != StoreOK {
		return errors.New("Store denied by host")
	}
	return nil
}

func (slave *Slave) Status() lib.Status {
	switch slave.remoteStatus() {
	case StatusDown:
		return lib.StatusDown
	case StatusReady:
		return lib.StatusReady
	case StatusStartup:
		return lib.StatusStartup
	case StatusShutdown:
		return lib.StatusShutdown
	default:
		return lib.StatusDown
	}
}

func (slave *Slave) Shutdown() error {
	response, err := slave.Queue(CommandShutdown)
	if err != nil {
		return err
	}
	if response.Arg(0) != ShutdownOK {
		return errors.New("Shutdown denied by host")
	}
	return nil
}

func (slave *Slave) Revision(rev lib.Revision) (lib.Revision, error) {
	cmd := CommandRevision
	if rev != nil {
		cmd = cmd.Param(rev.String())
	}
	response, err := slave.Queue(cmd)
	if err != nil {
		return nil, err
	}
	bytes, err := lib.ToRevision(response.Arg(0))
	if err != nil {
		return nil, err
	}
	return lib.Revision(bytes), nil
}

func (slave *Slave) Rebuild() error {
	response, err := slave.Queue(CommandRebuild)
	if err != nil {
		return err
	}
	if response.Arg(0) != RebuildOK {
		return errors.New("Rebuild denied by host")
	}
	return nil
}

// Master is a remotely connected master in the cluster.
type Master struct {
	*Slave
}

func (master *Master) Cluster() ([]lib.Node, error) {
	response, err := master.Queue(CommandCluster)
	if err != nil {
		return nil, err
	}
	nodes := make([]lib.Node, response.ArgCount())
	for i := 0; i < response.ArgCount(); i++ {
		nodes[i] = NewSlave(response.Arg(i))
	}
	return nodes, nil
}

func (master *Master) Replicas() ([]lib.Master, error) {
	response, err := master.Queue(CommandReplicas)
	if err != nil {
		return nil, err
	}
	replicas := make([]lib.Master, response.ArgCount())
	for i := 0; i < response.ArgCount(); i++ {
		replicas[i] = NewMaster(response.Arg(i))
	}
	return replicas, nil
}

func (slave *Slave) Role() (lib.Role, error) {
	response, err := slave.Queue(CommandRole)
	if err != nil {
		return lib.RoleSlave, err
	}
	switch response.Arg(0) {
	case RoleMasterPrimary:
		return lib.RoleMasterPrimary, nil
	case RoleMaster:
		return lib.RoleMaster, nil
	default:
		return lib.RoleSlave, nil
	}
}

func (slave *Slave) LocalKeys() ([]string, error) {
	response, err := slave.Queue(CommandLocalKeys.Param())
	if err != nil {
		return nil, err
	}
	return response.ArgList(), nil
}

func (slave *Slave) Keys() ([]string, error) {
	response, err := slave.Queue(CommandKeys.Param())
	if err != nil {
		return nil, err
	}
	return response.ArgList(), nil
}

func (slave *Slave) Mirror(peers []lib.Node) error {
	addrs := make([]string, len(peers))
	for i := range addrs {
		addrs[i] = peers[i].Address()
	}
	response, err := slave.Queue(CommandMirror.Param(addrs...))
	if err != nil {
		return err
	}
	if response.Arg(0) != MirrorOK {
		return errors.New("Expected OK, got " + response.Arg(0))
	}
	return nil
}

func (slave *Slave) Close() {
	if !slave.Dead {
		if slave.Connection != nil {
			slave.Connection.Close()
		}
		slave.Connection = nil
		slave.Dead = true
	}
}

func (master *Master) Join(n lib.Node) error {
	response, err := master.Queue(CommandJoin.Param(n.Address()))
	if err != nil {
		return err
	}
	if response.Arg(0) != JoinOK {
		return errors.New("Join denied by host")
	}
	return nil
}

func (master *Master) Assist(m lib.Master) error {
	response, err := master.Queue(CommandAssist.Param(m.Address()))
	if err != nil {
		return err
	}
	if response.Arg(0) != AssistOK {
		return errors.New("Assist denied by host")
	}
	return nil
}

// NewMaster initializes a new remote-connected master with the specified public address.
func NewMaster(addr string) *Master {
	return &Master{NewSlave(addr)}
}

// NewSlave initializes a new remote-connected slave with the specified public address.
func NewSlave(addr string) *Slave {
	return &Slave{PublicAddress: addr}
}
