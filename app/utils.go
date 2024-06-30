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

	length, err := strconv.Atoi(line)
	if err != nil {
		return "", err
	}

	data := make([]byte, length)
	_, err = reader.Read(data)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func write(c *net.Conn, value string) error {
	_, err := (*c).Write([]byte(value))
	return err
}

func FmtSimpleStr(value string) string {
	return fmt.Sprintf("+%s\r\n", value)
}

func FmtBulkStr(value string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func FmtNullBulkStr() string {
	return "$-1\r\n"
}

func FmtArray(items []string) string {
	array := fmt.Sprintf("*%d\r\n", len(items))
	for _, o := range items {
		array += fmt.Sprintf("$%d\r\n%s\r\n", len(o), o)
	}
	return array
}
