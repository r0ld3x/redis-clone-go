package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/r0ld3x/redis-clone-go/app/pkg/database"
)

func ParseRDB(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	data := make([]byte, 9)
	if _, err := io.ReadFull(file, data); err != nil {
		return err
	}
	if string(data[:5]) != "REDIS" {
		return errors.New("invalid RDB file: missing REDIS header")
	}
	version := string(data[5:])
	fmt.Println("RDB Version:", version)
	for {
		prefix := make([]byte, 1)
		_, err := file.Read(prefix)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		switch prefix[0] {
		case 0xFA:
			key, _ := readString(file)
			val, _ := readString(file)
			fmt.Printf("[Metadata] %s: %s\n", key, val)

		case 0xFE:
			dbIndex, _ := readLength(file)
			fmt.Printf("\n[Database] Selected DB: %d\n", dbIndex)

		case 0xFB:
			kvs, _ := readLength(file)
			exp, _ := readLength(file)
			fmt.Printf("[Database] KV Entries: %d, Expiring: %d\n", kvs, exp)

		case 0x00:
			key, _ := readString(file)
			val, _ := readString(file)
			// fmt.Printf("[SET] %s = %s (ex %d)\n", key, val, -1)
			database.SetKey(key, val, -1)

		case 0xFD:
			expTime := make([]byte, 4)
			if _, err := io.ReadFull(file, expTime); err != nil {
				return err
			}
			secs := binary.LittleEndian.Uint32(expTime)
			fmt.Printf("[Expire] Raw 0xFD: %d (unix seconds)\n", secs)

			nextType := make([]byte, 1)
			if _, err := file.Read(nextType); err != nil {
				return err
			}

			switch nextType[0] {
			case 0x00:
				key, _ := readString(file)
				val, _ := readString(file)
				// fmt.Printf("[Entry] Expiring key: %s = %s (ex %d)\n", key, val, secs)
				expireTime := time.Unix(int64(secs), 0)
				if !time.Now().After(expireTime) {
					database.SetKey(key, val, int(secs))
				}
			default:
				return fmt.Errorf("unexpected type after expire: 0x%X", nextType[0])
			}

		case 0xFC:
			expTime := make([]byte, 8)
			if _, err := io.ReadFull(file, expTime); err != nil {
				return err
			}
			expiry := binary.LittleEndian.Uint64(expTime)
			fmt.Printf("[Expire] Raw expiry: %d\n", expiry)

			// Read type of the next entry
			nextType := make([]byte, 1)
			if _, err := file.Read(nextType); err != nil {
				return err
			}

			switch nextType[0] {
			case 0x00:
				key, _ := readString(file)
				val, _ := readString(file)
				fmt.Printf("[Entry] Expiring key: %s = %s (px %d)\n", key, val, expiry)
				expireTime := time.UnixMilli(int64(expiry))
				isExpired := time.Now().After(expireTime)
				if !isExpired {
					database.SetKey(key, val, int(expiry))
				}
			default:
				return fmt.Errorf("unexpected type after expire: 0x%X", nextType[0])
			}

		case 0xFF:
			checksum := make([]byte, 8)
			file.Read(checksum)
			fmt.Println("[EOF] RDB file finished.")
			return nil

		default:
			return fmt.Errorf("unknown opcode: 0x%X", prefix[0])
		}
	}
	return nil
}
