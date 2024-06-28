package main

import (
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

func (st *state) ping(c net.Conn, command []string) error {
	if len(command) > 1 {
		return fmt.Errorf("PING does not expect extra arguments, got: %v", command)
	}

	return write(c, FmtSimpleStr("PONG"))
}

func (st *state) echo(c net.Conn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("ECHO expects 1 extra arguments, got: %v", command)
	}

	return write(c, FmtBulkStr(command[1]))
}

func (st *state) set(c net.Conn, command []string) error {
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

	st.db[command[1]] = entry

	return write(c, FmtSimpleStr("OK"))
}

func (st *state) get(c net.Conn, command []string) error {
	if len(command) != 2 {
		return fmt.Errorf("GET expects 1 extra arguments, got: %v", command)
	}

	entry, ok := st.db[command[1]]

	if !ok || (!entry.ExpiresAt.IsZero() && entry.ExpiresAt.Before(time.Now())) {
		return write(c, FmtNullBulkStr())
	}

	return write(c, FmtBulkStr(entry.Value))
}

func (st *state) info(c net.Conn, command []string) error {
	if len(command) > 2 {
		return fmt.Errorf("INFO expects at most 1 extra arguments, got: %v", command)
	}

	infos := []string{
		"# Replication",
	}

	if st.IsMaster() {
		infos = append(infos, []string{
			"role:master",
			fmt.Sprintf("master_replid:%s", st.replicationId),
			fmt.Sprintf("master_repl_offset:%d", st.replicationOffset),
		}...)
	} else {
		infos = append(infos, "role:slave")
	}

	data := strings.Join(infos, "\r\n")

	return write(c, FmtBulkStr(data))
}

func (st *state) replconf(c net.Conn, command []string) error {
	return write(c, FmtSimpleStr("OK"))
}
