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

type state struct {
	port                int
	replicationId       string
	replicationOffset   int
	replicasConnections []*net.Conn
	masterHost          string
	db                  map[string]dbEntry
}

func (s *state) IsMaster() bool {
	return s.masterHost == ""
}

func (s *state) ReplicateCommand(command []string) {
	for _, rc := range s.replicasConnections {
		_, err := fmt.Fprint(*rc, FmtArray(command))
		if err != nil {
			fmt.Printf("error replicating command to %v: %v\n", (*rc).RemoteAddr(), err)
		}
	}
}

func (s *state) ReplicaStartHandshake() error {
	parts := strings.Fields(s.masterHost)
	conn, err := net.Dial("tcp", net.JoinHostPort(parts[0], parts[1]))
	if err != nil {
		return err
	}

	reader := bufio.NewReader(conn)

	_, err = fmt.Fprint(conn, FmtArray([]string{"PING"}))
	if err != nil {
		return err
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	fmt.Printf("Handshake: PING response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("expected PONG to handshake PING, got %v", resp)
	}

	_, err = fmt.Fprint(conn, FmtArray([]string{"REPLCONF", "listening-port", fmt.Sprintf("%d", s.port)}))
	if err != nil {
		return err
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	fmt.Printf("Handshake: REPLCONF #1 response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected OK to handshake REPLCONF, got %v", resp)
	}

	_, err = fmt.Fprint(conn, FmtArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return err
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	fmt.Printf("Handshake: REPLCONF #2 response -> %v\n", resp)
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected OK to handshake REPLCONF, got %v", resp)
	}

	_, err = fmt.Fprint(conn, FmtArray([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return err
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	fmt.Printf("Handshake: PSYNC response -> %v\n", resp)

	respType, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if respType != '$' {
		return fmt.Errorf("expected $ as RDB file start, got %v", respType)
	}
	v, err := readBulkStr(reader)
	if err != nil {
		return err
	}
	fmt.Printf("Handshake: received RDB\n%q\n", v)

	go handleConnection(conn)
	return nil
}

var st state

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
		port:                *port,
		replicationId:       "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replicationOffset:   0,
		replicasConnections: make([]*net.Conn, 0),
		masterHost:          master,
		db:                  make(map[string]dbEntry),
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", st.port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", st.port)
	}
	defer l.Close()

	if !st.IsMaster() {
		err := st.ReplicaStartHandshake()
		if err != nil {
			log.Fatal(err)
		}
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(c net.Conn) {
	defer c.Close()
	reader := bufio.NewReader(c)

	for {
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
		fmt.Println("command:", command)
		if err := handleCommand(c, command); err != nil {
			fmt.Println("command error:", err.Error())
		}
	}
}

var commandFuncs = map[string]func(net.Conn, []string) error{
	"ping":     st.ping,
	"echo":     st.echo,
	"set":      st.set,
	"get":      st.get,
	"info":     st.info,
	"replconf": st.replconf,
	"psync":    st.psync,
}

func handleCommand(c net.Conn, command []string) error {
	f, ok := commandFuncs[strings.ToLower(command[0])]
	if !ok {
		return fmt.Errorf("got unexpected command: %v", command[0])
	}
	return f(c, command)
}
