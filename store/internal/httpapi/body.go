package httpapi

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/adammiribyan/wide_events/store/internal/protocol"
)

const maxCompressedBodyBytes = 512 << 10

var (
	errCompressedBodyTooLarge = errors.New("compressed request body exceeds limit")
	errDecodedBodyTooLarge    = errors.New("decoded request body exceeds limit")
	errUnsupportedEncoding    = errors.New("unsupported content encoding")
)

func readBoundedBody(response http.ResponseWriter, request *http.Request) ([]byte, error) {
	compressed := http.MaxBytesReader(response, request.Body, maxCompressedBodyBytes)
	defer compressed.Close()

	var reader io.Reader = compressed
	var zipper *gzip.Reader
	encoding := strings.ToLower(strings.TrimSpace(request.Header.Get("Content-Encoding")))
	switch encoding {
	case "", "identity":
	case "gzip":
		var err error
		zipper, err = gzip.NewReader(compressed)
		if err != nil {
			return nil, fmt.Errorf("open gzip body: %w", err)
		}
		defer zipper.Close()
		reader = zipper
	default:
		return nil, errUnsupportedEncoding
	}

	limit := int64(protocol.MaxDecodedBodyBytes + 1)
	if zipper == nil {
		limit = maxCompressedBodyBytes + 1
	}
	decoded, err := io.ReadAll(&io.LimitedReader{R: reader, N: limit})
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, errCompressedBodyTooLarge
		}
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(decoded) > protocol.MaxDecodedBodyBytes {
		return nil, errDecodedBodyTooLarge
	}
	if zipper == nil && len(decoded) > maxCompressedBodyBytes {
		return nil, errCompressedBodyTooLarge
	}
	return decoded, nil
}
