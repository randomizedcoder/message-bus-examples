package pool

import "fmt"

// Mode selects which pools are active for a run: -pool=none|messages|buffers|all
// (default all). The code paths are identical across modes; only reuse differs,
// so pool-on vs pool-off is a fair A/B (design §7.2).
type Mode int

const (
	ModeAll Mode = iota // default: message + buffer pools
	ModeNone
	ModeMessages // message pool only
	ModeBuffers  // buffer pool only
)

// ParseMode parses the -pool flag value.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "", "all":
		return ModeAll, nil
	case "none":
		return ModeNone, nil
	case "messages":
		return ModeMessages, nil
	case "buffers":
		return ModeBuffers, nil
	default:
		return ModeAll, fmt.Errorf("pool: unknown mode %q (want none|messages|buffers|all)", s)
	}
}

func (m Mode) String() string {
	switch m {
	case ModeNone:
		return "none"
	case ModeMessages:
		return "messages"
	case ModeBuffers:
		return "buffers"
	default:
		return "all"
	}
}

// BuffersEnabled reports whether the byte-buffer pool should be live.
func (m Mode) BuffersEnabled() bool { return m == ModeAll || m == ModeBuffers }

// MessagesEnabled reports whether typed message pools should be live.
func (m Mode) MessagesEnabled() bool { return m == ModeAll || m == ModeMessages }

// NewBufferPool returns a live *Buffers (over the given classes, or defaults)
// when buffers are enabled, else NopBuffers.
func (m Mode) NewBufferPool(classes ...int) BufferPool {
	if m.BuffersEnabled() {
		return NewBuffers(classes...)
	}
	return NopBuffers{}
}
