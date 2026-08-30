package trace

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Open opens a trace file, transparently decompressing .gz and .zst.
//
// zstd is handled by piping through the external `zstd` binary rather than a Go
// library, because the cacheMon dataset ships everything zstd-compressed but the
// standard library has no zstd and this module keeps a stdlib-only dependency
// set. If the binary is missing the error says so and suggests decompressing
// first.
func Open(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	switch {
	case strings.HasSuffix(path, ".gz"):
		zr, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("trace: gzip %s: %w", path, err)
		}
		return pair{zr, f}, nil

	case strings.HasSuffix(path, ".zst"), strings.HasSuffix(path, ".zstd"):
		f.Close()
		return openZstd(path)
	}

	return f, nil
}

func openZstd(path string) (io.ReadCloser, error) {
	bin, err := exec.LookPath("zstd")
	if err != nil {
		return nil, fmt.Errorf(
			"trace: %s is zstd-compressed but the `zstd` binary was not found on PATH; "+
				"install it (brew install zstd) or decompress the trace first", path)
	}

	cmd := exec.Command(bin, "-dc", path)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("trace: zstd pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("trace: start zstd: %w", err)
	}
	return &proc{out: out, cmd: cmd}, nil
}

// pair closes a decompressor and its underlying file together.
type pair struct {
	io.ReadCloser
	under io.Closer
}

func (p pair) Close() error {
	err := p.ReadCloser.Close()
	if cerr := p.under.Close(); err == nil {
		err = cerr
	}
	return err
}

// proc reads from a decompression subprocess.
type proc struct {
	out io.ReadCloser
	cmd *exec.Cmd
}

func (p *proc) Read(b []byte) (int, error) { return p.out.Read(b) }

func (p *proc) Close() error {
	p.out.Close()
	// The reader may stop early (--max-requests), leaving zstd writing into a
	// closed pipe; a non-zero exit is expected then and is not an error.
	_ = p.cmd.Wait()
	return nil
}

// LoadFile opens path and reads the whole trace.
func LoadFile(path string, p CSVParams, opts LoadOptions) ([]Request, ReadStats, error) {
	rc, err := Open(path)
	if err != nil {
		return nil, ReadStats{}, err
	}
	defer rc.Close()
	return ReadCSV(rc, p, opts)
}
