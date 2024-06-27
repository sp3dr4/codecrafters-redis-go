package main

import (
	"fmt"
	"net"
)

func ping(c net.Conn, command []string) error {
	if len(command) > 1 {
		return fmt.Errorf("PING does not expect extra arguments, got: %v", command)
	}
	if err := writeSimpleStr(c, "PONG"); err != nil {
		return err
	}
	return nil
}

func echo(c net.Conn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("ECHO expects 1 extra arguments, got: %v", command)
	}
	if err := writeBulkStr(c, command[1]); err != nil {
		return err
	}
	return nil
}

func set(c net.Conn, command []string) error {
	if len(command) != 3 {
		return fmt.Errorf("SET expects 2 extra arguments, got: %v", command)
	}
	db[command[1]] = command[2]
	if err := writeSimpleStr(c, "OK"); err != nil {
		return err
	}
	return nil
}

func get(c net.Conn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("GET expects 1 extra arguments, got: %v", command)
	}
	val, ok := db[command[1]]
	if !ok {
		if err := writeNullBulkStr(c); err != nil {
			return err
		}
	}
	if err := writeBulkStr(c, val); err != nil {
		return err
	}
	return nil
}
