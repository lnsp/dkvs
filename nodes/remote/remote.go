package remote

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"bufio"

	"github.com/lnsp/dkvs/nodes"
)

const (
	maxReconnects     = 5
	reconnectInterval = 3 * time.Second
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
	RebuildOK         = "OK"
	HandshakeOK       = "OK"
	AssistOK          = "OK"
	RevisionOK        = "OK"
	MirrorOK          = "OK"
)

var (
	CommandCluster    = &Command{Name: "CLUSTER"}
	CommandReplicas   = &Command{Name: "REPLICAS"}
	CommandRole       = &Command{Name: "ROLE"}
	CommandJoin       = &Command{Name: "JOIN"}
	CommandRevision   = &Command{Name: "REVISION"}
	CommandStore      = &Command{Name: "STORE"}
	CommandLocalStore = &Command{Name: "STORE_LOCAL"}
	CommandLocalRead  = &Command{Name: "READ_LOCAL"}
	CommandRead       = &Command{Name: "READ"}
	CommandError      = &Command{Name: "ERROR"}
	CommandStatus     = &Command{Name: "STATUS"}
	CommandShutdown   = &Command{Name: "SHUTDOWN"}
	CommandAddress    = &Command{Name: "ADDRESS"}
	CommandRebuild    = &Command{Name: "REBUILD"}
	CommandHandshake  = &Command{Name: "HANDSHAKE"}
	CommandAssist     = &Command{Name: "ASSIST"}
	CommandMirror     = &Command{Name: "MIRROR"}
	CommandLocalKeys  = &Command{Name: "KEYS_LOCAL"}
	CommandKeys       = &Command{Name: "KEYS"}
)

type Node interface {
	Close()
	Queue(cmd *Command) (*Command, error)
	Poll() (*Command, error)
	Push(*Command) error
}

func Error(err error) *Command {
	return CommandError.Param(strings.ToUpper(err.Error()))
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

func (cmd Command) ArgList() []string {
	return cmd.Args[:]
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

func (slave *Slave) Read(key string) (string, nodes.Revision, error) {
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

func (slave *Slave) LocalRead(key string) (string, nodes.Revision, error) {
	response, err := slave.Queue(CommandLocalRead.Param(key))
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

func (slave *Slave) LocalStore(key, value string, rev nodes.Revision) error {
	response, err := slave.Queue(CommandLocalStore.Param(key, value, rev.String()))
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
	response, err := slave.Queue(CommandShutdown)
	if err != nil {
		return err
	}
	if response.Arg(0) != ShutdownOK {
		return errors.New("Shutdown denied by host")
	}
	return nil
}

func (slave *Slave) Revision(rev nodes.Revision) (nodes.Revision, error) {
	cmd := CommandRevision
	if rev != nil {
		cmd = cmd.Param(rev.String())
	}
	response, err := slave.Queue(cmd)
	if err != nil {
		return nil, err
	}
	bytes, err := nodes.ToRevision(response.Arg(0))
	if err != nil {
		return nil, err
	}
	return nodes.Revision(bytes), nil
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

type Master struct {
	*Slave
}

func (master *Master) Cluster() ([]nodes.Slave, error) {
	response, err := master.Queue(CommandCluster)
	if err != nil {
		return nil, err
	}
	nodes := make([]nodes.Slave, response.ArgCount())
	for i := 0; i < response.ArgCount(); i++ {
		nodes[i] = NewSlave(response.Arg(i))
	}
	return nodes, nil
}

func (master *Master) Replicas() ([]nodes.Master, error) {
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

func (slave *Slave) Mirror(peers []nodes.Slave) error {
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

func (master *Master) Join(n nodes.Slave) error {
	response, err := master.Queue(CommandJoin.Param(n.Address()))
	if err != nil {
		return err
	}
	if response.Arg(0) != JoinOK {
		return errors.New("Join denied by host")
	}
	return nil
}

func (master *Master) Assist(m nodes.Master) error {
	response, err := master.Queue(CommandAssist.Param(m.Address()))
	if err != nil {
		return err
	}
	if response.Arg(0) != AssistOK {
		return errors.New("Assist denied by host")
	}
	return nil
}

func NewMaster(addr string) *Master {
	return &Master{NewSlave(addr)}
}

func NewSlave(addr string) *Slave {
	return &Slave{PublicAddress: addr}
}
