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
	port              int
	replicationId     string
	replicationOffset int
	masterHost        string
	db                map[string]dbEntry
}

func (s *state) IsMaster() bool {
	return s.masterHost == ""
}

func (s *state) ReplicaStartHandshake() error {
	parts := strings.Fields(s.masterHost)
	conn, err := net.Dial("tcp", net.JoinHostPort(parts[0], parts[1]))
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(conn, FmtArray([]string{"PING"}))
	return err
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
		port:              *port,
		replicationId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replicationOffset: 0,
		masterHost:        master,
		db:                make(map[string]dbEntry),
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", st.port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", st.port)
	}
	defer l.Close()

	if !st.IsMaster() {
		if err := st.ReplicaStartHandshake(); err != nil {
			log.Fatalf("failed handshake: %v", err)
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
			fmt.Println("expected '*' to start request, got:", respType)
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
		// fmt.Println("array.len:", numElements)

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
	"ping": st.ping,
	"echo": st.echo,
	"set":  st.set,
	"get":  st.get,
	"info": st.info,
}

func handleCommand(c net.Conn, command []string) error {
	f, ok := commandFuncs[strings.ToLower(command[0])]
	if !ok {
		return fmt.Errorf("got unexpected command: %v", command[0])
	}
	return f(c, command)
}
