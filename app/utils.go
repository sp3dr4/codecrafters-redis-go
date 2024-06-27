package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func readBulkStr(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	// fmt.Println("readBulkStr.line:", line)

	length, err := strconv.Atoi(line)
	if err != nil {
		return "", err
	}
	// fmt.Println("readBulkStr.length:", length)

	data := make([]byte, length)
	_, err = reader.Read(data)
	if err != nil {
		return "", err
	}
	dataStr := string(data)
	// fmt.Println("readBulkStr.data:", dataStr)

	// Read the trailing \r\n
	_, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return dataStr, nil
}

func writeSimpleStr(c net.Conn, value string) error {
	simple := fmt.Sprintf("+%s\r\n", value)
	_, err := c.Write([]byte(simple))
	return err
}

func writeBulkStr(c net.Conn, value string) error {
	bulk := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	_, err := c.Write([]byte(bulk))
	return err
}

func writeNullBulkStr(c net.Conn) error {
	_, err := c.Write([]byte("$-1\r\n"))
	return err
}
