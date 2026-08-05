package query

import (
	"errors"
	"io"
)

var errResponseTooLarge = errors.New("response would exceed byte limit")

type limitedWriter struct {
	writer  io.Writer
	limit   int64
	written int64
}

func newLimitedWriter(writer io.Writer, limit int64) *limitedWriter {
	return &limitedWriter{writer: writer, limit: limit}
}

func (writer *limitedWriter) Write(value []byte) (int, error) {
	if int64(len(value)) > writer.remaining() {
		return 0, errResponseTooLarge
	}
	n, err := writer.writer.Write(value)
	writer.written += int64(n)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (writer *limitedWriter) remaining() int64 { return writer.limit - writer.written }

func (writer *limitedWriter) canWrite(byteCount int) bool {
	return int64(byteCount) <= writer.remaining()
}
