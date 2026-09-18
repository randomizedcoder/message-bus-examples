// Package cli holds the tiny shared flag-parsing helper used by every
// message-bus client CLI (natscli, rabbitmqcli, mqttcli, valkeycli).
package cli

import (
	"flag"
	"fmt"
	"os"
)

// Flags are the options common to every client's pub/sub subcommands.
type Flags struct {
	Cmd     string // "pub" or "sub"
	Addr    string // host:port of the bus NodePort
	Subject string // subject / topic / channel / queue name
	Msg     string // message body (pub only)
	User    string // username (buses that authenticate)
	Pass    string // password (buses that authenticate)
}

// Parse reads os.Args as `<bin> <pub|sub> [flags]` and returns the
// resolved Flags. `defAddr` is the default NodePort address for this bus;
// `defPass` seeds -pass (typically from an env var).
func Parse(bin, defAddr, defPass string) *Flags {
	if len(os.Args) < 2 {
		Usage(bin)
	}
	cmd := os.Args[1]
	if cmd != "pub" && cmd != "sub" {
		Usage(bin)
	}
	fs := flag.NewFlagSet(bin+" "+cmd, flag.ExitOnError)
	f := &Flags{Cmd: cmd}
	fs.StringVar(&f.Addr, "addr", defAddr, "bus host:port (NodePort)")
	fs.StringVar(&f.Subject, "subject", "demo", "subject / topic / channel / queue")
	fs.StringVar(&f.Msg, "msg", "hello from "+bin, "message body (pub)")
	fs.StringVar(&f.User, "user", "admin", "username")
	fs.StringVar(&f.Pass, "pass", defPass, "password")
	_ = fs.Parse(os.Args[2:])
	return f
}

// Usage prints the standard usage line and exits non-zero.
func Usage(bin string) {
	fmt.Fprintf(os.Stderr,
		"usage: %s <pub|sub> [-addr host:port] [-subject name] [-msg text] [-user u] [-pass p]\n", bin)
	os.Exit(2)
}
