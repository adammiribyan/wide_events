package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// RunClient invokes one local administration command. It returns a
// conventional process exit code: 0 success, 1 operation failure, 2 usage.
func RunClient(socket string, args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || args[0] != "backup" {
		_, _ = fmt.Fprintln(stderr, "usage: wide-events admin backup")
		return 2
	}
	connection, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "connect to admin socket: %v\n", err)
		return 1
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(connectionDeadline))
	if err := json.NewEncoder(connection).Encode(command{Command: "backup"}); err != nil {
		_, _ = fmt.Fprintf(stderr, "send admin command: %v\n", err)
		return 1
	}
	var result response
	if err := decodeResponse(connection, &result); err != nil {
		_, _ = fmt.Fprintf(stderr, "read admin response: %v\n", err)
		return 1
	}
	if !result.OK {
		if result.RequiredBytes > 0 || result.AvailableBytes > 0 {
			_, _ = fmt.Fprintf(stderr, "%s (required=%d available=%d)\n", result.Error, result.RequiredBytes, result.AvailableBytes)
		} else {
			_, _ = fmt.Fprintln(stderr, result.Error)
		}
		return 1
	}
	_, _ = fmt.Fprintln(stdout, result.Path)
	return 0
}

func decodeResponse(reader io.Reader, result *response) error {
	payload, err := io.ReadAll(io.LimitReader(reader, maximumCommandBytes+1))
	if err != nil {
		return err
	}
	if len(payload) > maximumCommandBytes {
		return fmt.Errorf("admin response exceeds %d bytes", maximumCommandBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("admin response contains a trailing value")
		}
		return err
	}
	return nil
}
