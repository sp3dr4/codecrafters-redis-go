package main

import (
	"bufio"
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
