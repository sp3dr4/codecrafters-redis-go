package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
)

type RedisConn struct {
	FromMaster bool
	conn       *net.Conn
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

var commandFuncs = map[string]func(RedisConn, []string) error{
	"ping":     st.ping,
	"echo":     st.echo,
	"set":      st.set,
	"get":      st.get,
	"info":     st.info,
	"replconf": st.replconf,
	"psync":    st.psync,
}

func (s *state) IsMaster() bool {
	return s.replica.masterHost == ""
}

func (s *state) ReplicateCommand(command []string) {
	for _, rc := range s.master.replicasConnections {
		_, err := fmt.Fprint(*rc, FmtArray(command))
		if err != nil {
			fmt.Printf("error replicating command to %v: %v\n", (*rc).RemoteAddr(), err)
		}
	}
}

func (s *state) ReplicaStartHandshake(rc RedisConn) error {
	conn := *rc.conn
	reader := bufio.NewReader(conn)

	_, err := fmt.Fprint(conn, FmtArray([]string{"PING"}))
	if err != nil {
		return err
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	// fmt.Printf("Handshake: PING response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("expected PONG to handshake PING, got %v", resp)
	}

	// fmt.Println("Handshake: sending REPLCONF #1")
	_, err = fmt.Fprint(conn, FmtArray([]string{"REPLCONF", "listening-port", fmt.Sprintf("%d", s.port)}))
	if err != nil {
		fmt.Println("error sending REPLCONF #1", err)
		return err
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("error reading REPLCONF #1 response", err)
		return err
	}
	// fmt.Printf("Handshake: REPLCONF #1 response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected OK to handshake REPLCONF, got %v", resp)
	}

	// fmt.Println("Handshake: sending REPLCONF #2")
	_, err = fmt.Fprint(conn, FmtArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		fmt.Println("error sending REPLCONF #2", err)
		return err
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("error reading REPLCONF #2 response", err)
		return err
	}
	// fmt.Printf("Handshake: REPLCONF #2 response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected OK to handshake REPLCONF, got %v", resp)
	}

	// fmt.Println("Handshake: sending PSYNC")
	_, err = fmt.Fprint(conn, FmtArray([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		fmt.Println("error sending PSYNC", err)
		return err
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("error reading PSYNC response", err)
		return err
	}
	// fmt.Printf("Handshake: PSYNC response -> %v\n", resp)

	fmt.Println("Handshake: reading RDB start byte")
	respType, err := reader.ReadByte()
	if err != nil {
		fmt.Println("error reading RDB start byte", err)
		return err
	}
	// fmt.Printf("Handshake: RDB start byte -> %v\n", string(respType))
	if respType != '$' {
		return fmt.Errorf("expected $ as RDB file start, got %v", respType)
	}
	// fmt.Println("Handshake: reading RDB file content")
	v, err := readBulkStr(reader)
	if err != nil {
		fmt.Println("error reading RDB file content", err)
		return err
	}
	fmt.Printf("Handshake: received RDB\n%q\n\n", v)

	return nil
}

func (s *state) handleCommand(c RedisConn, command []string) error {
	fmt.Printf("command: %v\n", command)
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
		fmt.Printf("%v: increasing offset by %d\n", command, commandBytes)
		s.replica.processedOffset += commandBytes
	}
	return nil
}

func (s *state) handleConnection(c RedisConn) {
	conn := *c.conn
	defer func(c *net.Conn) {
		fmt.Println("closing conn")
		(*c).Close()
	}(c.conn)

	reader := bufio.NewReader(conn)

	for {
		fmt.Printf("[replica-conn:%t] handleConnection loop\n", c.FromMaster)
		respType, err := reader.ReadByte()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("error reading request first byte:", err.Error())
			}
			return
		}
		if respType != '*' {
			fmt.Println("expected '*' to start request, got:", string(respType))
			return
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("error handling array:", err.Error())
			return
		}
		line = strings.TrimSpace(line)
		numElements, err := strconv.Atoi(line)
		if err != nil {
			fmt.Println("error getting array size:", err.Error())
			return
		}

		command := make([]string, numElements)
		for i := range numElements {
			argType, err := reader.ReadByte()
			if err != nil {
				fmt.Printf("%d. error reading type byte: %s", i, err.Error())
				return
			}

			switch argType {
			case '$':
				v, err := readBulkStr(reader)
				if err != nil {
					fmt.Printf("%d. error reading bulk string: %s", i, err.Error())
					return
				}
				// Read the trailing \r\n
				_, err = reader.ReadString('\n')
				if err != nil {
					fmt.Printf("%d. error reading trailing newline: %s", i, err.Error())
					return
				}
				command[i] = v
			default:
				fmt.Printf("%d. got not implemented type %v", i, string(argType))
				return
			}
		}

		if err := st.handleCommand(c, command); err != nil {
			fmt.Println("command error:", err.Error())
		}
	}
}

func main() {
	fmt.Println("Start main!")
	port := flag.Int("port", 6379, "Port to bind to. Defaults to 6379")
	replicaof := flag.String("replicaof", "", "Address and port of master")
	flag.Parse()

	master := ""
	if *replicaof != "" {
		match, _ := regexp.MatchString(`^\w+ \d+$`, *replicaof)
		if !match {
			log.Fatalf("invalid replicaof %s", *replicaof)
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

	if !st.IsMaster() {
		parts := strings.Fields(st.replica.masterHost)
		conn, err := net.Dial("tcp", net.JoinHostPort(parts[0], parts[1]))
		if err != nil {
			log.Fatal(err)
		}
		rc := RedisConn{FromMaster: true, conn: &conn}

		if err = st.ReplicaStartHandshake(rc); err != nil {
			log.Fatal(err)
		}

		go st.handleConnection(rc)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", st.port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", st.port)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go st.handleConnection(RedisConn{FromMaster: false, conn: &conn})
	}
}
