package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type dbEntry struct {
	Value     string
	ExpiresAt time.Time
}

func (s *state) ping(c *RedisConn, command []string) error {
	if len(command) > 1 {
		return fmt.Errorf("PING does not expect extra arguments, got: %v", command)
	}

	if c.FromMaster {
		return nil
	}
	return write(c.conn, FmtSimpleStr("PONG"))
}

func (s *state) echo(c *RedisConn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("ECHO expects 1 extra arguments, got: %v", command)
	}

	if c.FromMaster {
		return nil
	}
	return write(c.conn, FmtBulkStr(command[1]))
}

func (s *state) set(c *RedisConn, command []string) error {
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

	if c.FromMaster {
		return nil
	}
	defer s.ReplicateCommand(command)
	return write(c.conn, FmtSimpleStr("OK"))
}

func (s *state) get(c *RedisConn, command []string) error {
	if c.FromMaster {
		return nil
	}

	if len(command) != 2 {
		return fmt.Errorf("GET expects 1 extra arguments, got: %v", command)
	}

	entry, ok := s.db[command[1]]

	if !ok || (!entry.ExpiresAt.IsZero() && entry.ExpiresAt.Before(time.Now())) {
		return write(c.conn, FmtNullBulkStr())
	}

	return write(c.conn, FmtBulkStr(entry.Value))
}

func (s *state) info(c *RedisConn, command []string) error {
	if c.FromMaster {
		return nil
	}

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

	return write(c.conn, FmtBulkStr(strings.Join(infos, "\r\n")))
}

func (s *state) replconf(c *RedisConn, command []string) error {
	if c.FromMaster {
		return write(c.conn, FmtArray([]string{"REPLCONF", "ACK", fmt.Sprint(s.replica.processedOffset)}))
	}

	if s.IsMaster() {
		return write(c.conn, FmtSimpleStr("OK"))
	}

	return nil
}

func (s *state) psync(c *RedisConn, command []string) error {
	err := write(c.conn, FmtSimpleStr(fmt.Sprintf("FULLRESYNC %s 0", s.master.replicationId)))
	if err != nil {
		return err
	}
	emptyRDBHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	emptyRDB, err := hex.DecodeString(emptyRDBHex)
	if err != nil {
		return err
	}
	_, err = (*c.conn).Write([]byte(fmt.Sprintf("$%d\r\n", len(emptyRDB))))
	if err != nil {
		return err
	}
	_, err = (*c.conn).Write(emptyRDB)
	if err != nil {
		return err
	}

	s.master.replicasConnections = append(s.master.replicasConnections, c.conn)

	return nil
}
