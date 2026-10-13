package cluster

import "strings"

const SlotCount = 16384

// HashSlot maps a key to one of Redis Cluster's 16384 logical slots.
// If a non-empty {...} hash tag exists, only the tag bytes participate.
func HashSlot(key string) int {
	return int(crc16([]byte(hashKey(key))) % SlotCount)
}

func hashKey(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 || open+1 >= len(key) {
		return key
	}
	closeRel := strings.IndexByte(key[open+1:], '}')
	if closeRel <= 0 {
		return key
	}
	return key[open+1 : open+1+closeRel]
}

// crc16 implements CRC-16/XMODEM, the polynomial used by Redis Cluster.
func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
