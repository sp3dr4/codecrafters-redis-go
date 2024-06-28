package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

var db = make(map[string]dbEntry)

func main() {
	fmt.Println("Start main!")
	port := flag.Int("port", 6379, "Port to bind to. Defaults to 6379")
	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", *port)
	}
	defer l.Close()

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
	"ping": ping,
	"echo": echo,
	"set":  set,
	"get":  get,
	"info": info,
}

func handleCommand(c net.Conn, command []string) error {
	f, ok := commandFuncs[strings.ToLower(command[0])]
	if !ok {
		return fmt.Errorf("got unexpected command: %v", command[0])
	}
	return f(c, command)
}
