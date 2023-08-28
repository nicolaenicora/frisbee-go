package frisbee

import (
	"github.com/loopholelabs/frisbee-go/pkg/packet"
	"sync"
)

type Packets struct {
	mu sync.Mutex
	d  []*packet.Packet
}

func NewPackets() *Packets {
	return &Packets{
		d: make([]*packet.Packet, 0),
	}
}

func (s *Packets) Poll() *packet.Packet {
	var p *packet.Packet

	s.mu.Lock()
	if len(s.d) > 0 {
		p, s.d = s.d[0], s.d[1:]
	}
	s.mu.Unlock()
	return p
}

func (s *Packets) Set(packets []*packet.Packet) {
	s.mu.Lock()
	s.d = packets
	s.mu.Unlock()
}
