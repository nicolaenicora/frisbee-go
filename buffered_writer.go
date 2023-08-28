package frisbee

import (
	"bufio"
	"io"
	"sync"
)

type BufferedWriter struct {
	w *bufio.Writer

	mu sync.RWMutex
}

func NewBufferedWriterSize(w io.Writer, size int) *BufferedWriter {
	return &BufferedWriter{
		w:  bufio.NewWriterSize(w, size),
		mu: sync.RWMutex{},
	}
}
func (bw *BufferedWriter) Buffered() int {
	bw.mu.RLock()
	n := bw.w.Buffered()
	bw.mu.RUnlock()
	return n
}
func (bw *BufferedWriter) Write(p []byte) (int, error) {
	bw.mu.Lock()
	n, err := bw.w.Write(p)
	bw.mu.Unlock()
	return n, err
}
func (bw *BufferedWriter) Flush() error {
	bw.mu.Lock()
	err := bw.w.Flush()
	bw.mu.Unlock()
	return err
}
