package resp

import "fmt"

type Protocol uint8

const (
	Protocol2 Protocol = 2
	Protocol3 Protocol = 3
)

func (p Protocol) String() string {
	switch p {
	case Protocol2:
		return "2"
	case Protocol3:
		return "3"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

func ValidProtocol(version int) (Protocol, bool) {
	switch version {
	case 2:
		return Protocol2, true
	case 3:
		return Protocol3, true
	default:
		return 0, false
	}
}
