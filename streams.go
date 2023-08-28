package frisbee

import "sync"

type Streams struct {
	mu sync.RWMutex
	d  map[uint16]*Stream
}

func NewStreams() *Streams {
	return &Streams{
		d: make(map[uint16]*Stream),
	}
}

func (s *Streams) Remove(id uint16) {
	s.mu.Lock()
	delete(s.d, id)
	s.mu.Unlock()
}

func (s *Streams) Get(id uint16) *Stream {
	s.mu.RLock()
	stream := s.d[id]
	s.mu.RUnlock()
	return stream
}

func (s *Streams) CreateWithCheckOfExistence(id uint16, f func() *Stream) *Stream {
	s.mu.Lock()
	var stream *Stream
	if stream = s.d[id]; stream == nil {
		stream = f()
		s.d[id] = stream
	}
	s.mu.Unlock()

	return stream
}

func (s *Streams) Create(id uint16, f func() *Stream) *Stream {
	s.mu.Lock()

	stream := f()
	s.d[id] = stream
	s.mu.Unlock()

	return stream
}

func (s *Streams) CloseAll() {
	s.mu.Lock()
	for _, stream := range s.d {
		_ = stream.Close()
	}
	s.mu.Unlock()
}
