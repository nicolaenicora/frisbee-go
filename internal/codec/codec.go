package codec

import (
	"encoding/binary"
	"github.com/loophole-labs/frisbee/internal/protocol"
	"github.com/panjf2000/gnet"
	"github.com/pkg/errors"
)

type Packet struct {
	Message *protocol.MessageV0
	Content []byte
}

type ICodec struct {
	Packets map[uint32]*Packet
}

// Encode for gnet codec
func (codec *ICodec) Encode(_ gnet.Conn, buf []byte) ([]byte, error) {
	return buf, nil
}

// Encode for gnet codec
func (codec *ICodec) Decode(c gnet.Conn) ([]byte, error) {
	buffer := c.Read()
	if protocol.HeaderLengthV0 > len(buffer) {
		return nil, errors.New("invalid message length")
	}
	decodedMessage, err := protocol.DecodeV0(buffer[:protocol.HeaderLengthV0])
	if err != nil {
		c.ResetBuffer()
		return nil, errors.Wrap(err, "error decoding header")
	}
	key := [4]byte{}
	binary.BigEndian.PutUint32(key[:], decodedMessage.Id)
	packet := &Packet{
		Message: &decodedMessage,
	}
	if decodedMessage.ContentLength > 0 {
		if int(decodedMessage.ContentLength+protocol.HeaderLengthV0) > len(buffer) {
			return nil, errors.New("invalid content length")
		}
		packet.Content = buffer[protocol.HeaderLengthV0:decodedMessage.ContentLength]
		codec.Packets[decodedMessage.Id] = packet
		c.ShiftN(int(decodedMessage.ContentLength + protocol.HeaderLengthV0))
		return key[:], nil
	}
	codec.Packets[decodedMessage.Id] = packet
	c.ShiftN(protocol.HeaderLengthV0)
	return key[:], nil
}
