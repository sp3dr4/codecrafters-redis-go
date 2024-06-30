package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type dbEntry struct {
	Value     string
	ExpiresAt time.Time
}

func (s *state) ping(c *net.Conn, command []string) error {
	if len(command) > 1 {
		return fmt.Errorf("PING does not expect extra arguments, got: %v", command)
	}

	if s.FromMaster(c) {
		return nil
	}
	return write(c, FmtSimpleStr("PONG"))
}

func (s *state) echo(c *net.Conn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("ECHO expects 1 extra arguments, got: %v", command)
	}

	if s.FromMaster(c) {
		return nil
	}
	return write(c, FmtBulkStr(command[1]))
}

func (s *state) set(c *net.Conn, command []string) error {
	if len(command) != 3 && len(command) != 5 {
		return fmt.Errorf("SET expects 2 or 4 extra arguments, got: %v", command)
	}

	entry := dbEntry{Value: command[2]}
	if len(command) == 5 {
		if strings.ToLower(command[3]) != "px" {
			return fmt.Errorf("SET expects PX as optional extra argument, got: %v", command)
		}
		px, err := strconv.Atoi(command[4])
		if err != nil {
			return err
		}
		entry.ExpiresAt = time.Now().Add(time.Duration(px) * time.Millisecond)
	}

	s.db[command[1]] = entry

	if s.FromMaster(c) {
		return nil
	}
	defer s.ReplicateCommand(command)
	return write(c, FmtSimpleStr("OK"))
}

func (s *state) get(c *net.Conn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("GET expects 1 extra arguments, got: %v", command)
	}

	entry, ok := s.db[command[1]]

	if !ok || (!entry.ExpiresAt.IsZero() && entry.ExpiresAt.Before(time.Now())) {
		return write(c, FmtNullBulkStr())
	}

	if s.FromMaster(c) {
		return nil
	}
	return write(c, FmtBulkStr(entry.Value))
}

func (s *state) info(c *net.Conn, command []string) error {
	if len(command) > 2 {
		return fmt.Errorf("INFO expects at most 1 extra arguments, got: %v", command)
	}

	infos := []string{
		"# Replication",
	}

	if s.IsMaster() {
		infos = append(infos, []string{
			"role:master",
			fmt.Sprintf("master_replid:%s", s.master.replicationId),
			fmt.Sprintf("master_repl_offset:%d", s.master.replicationOffset),
		}...)
	} else {
		infos = append(infos, "role:slave")
	}

	if s.FromMaster(c) {
		return nil
	}
	return write(c, FmtBulkStr(strings.Join(infos, "\r\n")))
}

func (s *state) replconf(c *net.Conn, command []string) error {
	if s.IsMaster() {
		return write(c, FmtSimpleStr("OK"))
	}

	// TODO: if !s.FromMaster -> err

	if strings.ToLower(command[1]) != "getack" {
		return fmt.Errorf("replica REPLCONF expects GETACK, got: %v", command)
	}
	return write(c, FmtArray([]string{"REPLCONF", "ACK", fmt.Sprint(s.replica.processedOffset)}))
}

func (s *state) psync(c *net.Conn, command []string) error {
	err := write(c, FmtSimpleStr(fmt.Sprintf("FULLRESYNC %s 0", s.master.replicationId)))
	if err != nil {
		return err
	}
	emptyRDBHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	emptyRDB, err := hex.DecodeString(emptyRDBHex)
	if err != nil {
		return err
	}
	_, err = (*c).Write([]byte(fmt.Sprintf("$%d\r\n", len(emptyRDB))))
	if err != nil {
		return err
	}
	_, err = (*c).Write(emptyRDB)
	if err != nil {
		return err
	}

	s.master.replicasConnections = append(s.master.replicasConnections, c)

	return nil
}
