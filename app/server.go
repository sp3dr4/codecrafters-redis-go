package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var logger = log.New(os.Stdout, " > ", log.Ldate|log.Ltime|log.Lmicroseconds)

type RedisConn struct {
	FromMaster bool
	conn       *net.Conn
	connReader *bufio.Reader
}

func (rc *RedisConn) ReadString(delim byte) (string, error) {
	return rc.connReader.ReadString(delim)
}

func (rc *RedisConn) ReadByte() (byte, error) {
	return rc.connReader.ReadByte()
}

func NewRedisConn(fromMaster bool, conn *net.Conn) *RedisConn {
	return &RedisConn{
		FromMaster: fromMaster,
		conn:       conn,
		connReader: bufio.NewReader(*conn),
	}
}

type masterConf struct {
	replicationId       string
	replicationOffset   int
	replicasConnections []*net.Conn
}

type replicaConf struct {
	masterHost      string
	processedOffset int
}

type state struct {
	port    int
	master  masterConf
	replica replicaConf
	db      map[string]dbEntry
}

var st state

var commandFuncs = map[string]func(*RedisConn, []string) error{
	"ping":     st.ping,
	"echo":     st.echo,
	"set":      st.set,
	"get":      st.get,
	"info":     st.info,
	"replconf": st.replconf,
	"psync":    st.psync,
	"wait":     st.wait,
}

func (s *state) IsMaster() bool {
	return s.replica.masterHost == ""
}

func (s *state) ReplicateCommand(command []string) {
	for _, rc := range s.master.replicasConnections {
		_, err := fmt.Fprint(*rc, FmtArray(command))
		if err != nil {
			logger.Printf("error replicating command to %v: %v\n", (*rc).RemoteAddr(), err)
		}
	}
}

func (s *state) ReplicaStartHandshake(rc *RedisConn) error {
	conn := *rc.conn

	_, err := fmt.Fprint(conn, FmtArray([]string{"PING"}))
	if err != nil {
		return err
	}
	resp, err := rc.ReadString('\n')
	if err != nil {
		return err
	}
	logger.Printf("Handshake: PING response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("expected PONG to handshake PING, got %v", resp)
	}

	logger.Println("Handshake: sending REPLCONF #1")
	_, err = fmt.Fprint(conn, FmtArray([]string{"REPLCONF", "listening-port", fmt.Sprintf("%d", s.port)}))
	if err != nil {
		logger.Println("error sending REPLCONF #1", err)
		return err
	}
	resp, err = rc.ReadString('\n')
	if err != nil {
		logger.Println("error reading REPLCONF #1 response", err)
		return err
	}
	logger.Printf("Handshake: REPLCONF #1 response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected OK to handshake REPLCONF, got %v", resp)
	}

	logger.Println("Handshake: sending REPLCONF #2")
	_, err = fmt.Fprint(conn, FmtArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		logger.Println("error sending REPLCONF #2", err)
		return err
	}
	resp, err = rc.ReadString('\n')
	if err != nil {
		logger.Println("error reading REPLCONF #2 response", err)
		return err
	}
	logger.Printf("Handshake: REPLCONF #2 response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected OK to handshake REPLCONF, got %v", resp)
	}

	logger.Println("Handshake: sending PSYNC")
	_, err = fmt.Fprint(conn, FmtArray([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		logger.Println("error sending PSYNC", err)
		return err
	}
	resp, err = rc.ReadString('\n')
	if err != nil {
		logger.Println("error reading PSYNC response", err)
		return err
	}
	logger.Printf("Handshake: PSYNC response -> %v\n", resp)

	logger.Println("Handshake: reading RDB start byte")
	respType, err := rc.ReadByte()
	if err != nil {
		logger.Println("error reading RDB start byte", err)
		return err
	}
	if respType != '$' {
		return fmt.Errorf("expected $ as RDB file start, got %v", respType)
	}
	v, err := readBulkStr(rc.connReader)
	if err != nil {
		logger.Println("error reading RDB file content", err)
		return err
	}
	logger.Printf("Handshake: received RDB\n%q\n\n", v)

	return nil
}

func (s *state) handleCommand(c *RedisConn, command []string) error {
	logger.Printf("command: %v\n", command)
	f, ok := commandFuncs[strings.ToLower(command[0])]
	if !ok {
		return fmt.Errorf("got unexpected command: %v", command[0])
	}
	err := f(c, command)
	if err != nil {
		return err
	}
	if c.FromMaster {
		commandBytes := len(FmtArray(command))
		logger.Printf("%v: increasing offset by %d\n", command, commandBytes)
		s.replica.processedOffset += commandBytes
	}
	return nil
}

func handleConnection(c *RedisConn) {
	defer func(c *net.Conn) {
		logger.Println("closing conn")
		(*c).Close()
	}(c.conn)

	for {
		logger.Printf("[from-master:%t] handleConnection loop start\n", c.FromMaster)
		respType, err := c.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.Println("EOF")
				break
			}
			logger.Println("error reading request first byte:", err.Error())
			return
		}
		logger.Printf("[from-master:%t] type: %v\n", c.FromMaster, string(respType))
		if respType != '*' {
			logger.Println("expected '*' to start request, got:", string(respType))
			return
		}

		line, err := c.ReadString('\n')
		if err != nil {
			logger.Println("error handling array:", err.Error())
			return
		}
		line = strings.TrimSpace(line)
		numElements, err := strconv.Atoi(line)
		if err != nil {
			logger.Println("error getting array size:", err.Error())
			return
		}

		logger.Printf("[from-master:%t] parsing command array of %d elements\n", c.FromMaster, numElements)
		command := make([]string, numElements)
		for i := range numElements {
			argType, err := c.ReadByte()
			if err != nil {
				logger.Printf("%d. error reading type byte: %s", i, err.Error())
				return
			}

			switch argType {
			case '$':
				v, err := readBulkStr(c.connReader)
				if err != nil {
					logger.Printf("%d. error reading bulk string: %s", i, err.Error())
					return
				}
				// Read the trailing \r\n
				_, err = c.ReadString('\n')
				if err != nil {
					logger.Printf("%d. error reading trailing newline: %s", i, err.Error())
					return
				}
				command[i] = v
			default:
				logger.Printf("%d. got not implemented type %v", i, string(argType))
				return
			}
		}

		if err := st.handleCommand(c, command); err != nil {
			logger.Println("command error:", err.Error())
		}
	}
}

func main() {
	logger.Println("Start main!")
	port := flag.Int("port", 6379, "Port to bind to. Defaults to 6379")
	replicaof := flag.String("replicaof", "", "Address and port of master")
	flag.Parse()

	master := ""
	if *replicaof != "" {
		match, _ := regexp.MatchString(`^\w+ \d+$`, *replicaof)
		if !match {
			logger.Fatalf("invalid replicaof %s", *replicaof)
		}
		master = *replicaof
	}
	st = state{
		port: *port,
		master: masterConf{
			replicationId:       "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			replicationOffset:   0,
			replicasConnections: make([]*net.Conn, 0),
		},
		replica: replicaConf{
			masterHost:      master,
			processedOffset: 0,
		},
		db: make(map[string]dbEntry),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", st.port))
	if err != nil {
		logger.Fatalf("Failed to bind to port %d", st.port)
	}
	defer listener.Close()

	if !st.IsMaster() {
		parts := strings.Fields(st.replica.masterHost)
		handConn, err := net.Dial("tcp", net.JoinHostPort(parts[0], parts[1]))
		if err != nil {
			logger.Fatal(err)
		}
		rc := NewRedisConn(true, &handConn)

		if err = st.ReplicaStartHandshake(rc); err != nil {
			logger.Fatal("error during handshake:", err)
		}
		logger.Println("finished handshake")

		go handleConnection(rc)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Println("error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(NewRedisConn(false, &conn))
	}
}
