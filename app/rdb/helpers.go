package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func readLength(r io.Reader) (int, error) {
	b := make([]byte, 1)
	if _, err := r.Read(b); err != nil {
		return 0, err
	}
	switch b[0] >> 6 {
	case 0b00:
		return int(b[0] & 0x3F), nil
	case 0b01:
		b2 := make([]byte, 1)
		r.Read(b2)
		return ((int(b[0] & 0x3F)) << 8) | int(b2[0]), nil
	case 0b10:
		b4 := make([]byte, 4)
		r.Read(b4)
		return int(binary.BigEndian.Uint32(b4)), nil
	default:
		return 0, errors.New("unsupported length encoding (0b11)")
	}
}

func readString(r io.Reader) (string, error) {
	b := make([]byte, 1)
	if _, err := r.Read(b); err != nil {
		return "", err
	}
	switch b[0] >> 6 {
	case 0b00:
		length := int(b[0] & 0x3F)
		buf := make([]byte, length)
		_, err := io.ReadFull(r, buf)
		return string(buf), err
	case 0b01:
		b2 := make([]byte, 1)
		r.Read(b2)
		length := ((int(b[0] & 0x3F)) << 8) | int(b2[0])
		buf := make([]byte, length)
		_, err := io.ReadFull(r, buf)
		return string(buf), err
	case 0b10:
		b4 := make([]byte, 4)
		r.Read(b4)
		length := int(binary.BigEndian.Uint32(b4))
		buf := make([]byte, length)
		_, err := io.ReadFull(r, buf)
		return string(buf), err
	case 0b11:
		encType := b[0] & 0x3F
		switch encType {
		case 0:
			b := make([]byte, 1)
			r.Read(b)
			return fmt.Sprintf("%d", int8(b[0])), nil
		case 1:
			b := make([]byte, 2)
			r.Read(b)
			return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(b))), nil
		case 2:
			b := make([]byte, 4)
			r.Read(b)
			return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(b))), nil
		case 3:
			return "", errors.New("LZF compression not supported")
		default:
			return "", fmt.Errorf("unknown special string encoding: 0x%X", encType)
		}
	default:
		return "", errors.New("invalid string encoding prefix")
	}
}
