package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Start main!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
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
			fmt.Println("error reading request first byte:", err.Error())
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

func handleCommand(c net.Conn, command []string) error {
	switch strings.ToLower(command[0]) {
	case "ping":
		if len(command) > 1 {
			return fmt.Errorf("PING does not expect extra arguments, got: %v", command)
		}
		_, err := c.Write([]byte("+PONG\r\n"))
		if err != nil {
			return err
		}
	case "echo":
		if len(command) != 2 {
			return fmt.Errorf("ECHO expects one extra arguments, got: %v", command)
		}
		toEcho := command[1]
		_, err := c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(toEcho), toEcho)))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("got unexpected command: %v", command[0])
	}
	return nil
}
