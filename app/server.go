package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Start main!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func(c net.Conn) {
			for i := 0; i < 3; {
				d := make([]byte, 128)
				_, err = c.Read(d)
				if err != nil {
					fmt.Println("Error writing response: ", err.Error())
				}

				_, err = c.Write([]byte("+PONG\r\n"))
				if err != nil {
					fmt.Println("Error writing response: ", err.Error())
				}
				i++
			}
			c.Close()
		}(conn)
	}
}
