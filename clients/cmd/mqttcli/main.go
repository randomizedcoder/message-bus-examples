// mqttcli — a minimal MQTT pub/sub client for the Mosquitto bridge cluster.
//
//	mqttcli pub -addr 127.0.0.1:30883 -subject demo/topic -msg "hello"
//	mqttcli sub -addr 127.0.0.1:30883 -subject demo/topic
//
// The brokers allow anonymous connections (lab config). Because the three
// Mosquitto pods are full-mesh bridged, a publish that lands on any broker
// is delivered to subscribers connected to the others.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
)

func main() {
	f := cli.Parse("mqttcli", "127.0.0.1:30883", "")

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + f.Addr).
		SetClientID(fmt.Sprintf("mqttcli-%d", os.Getpid())).
		SetAutoReconnect(true).
		SetConnectRetry(true)
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalf("connect: %v", tok.Error())
	}
	defer c.Disconnect(250)

	switch f.Cmd {
	case "pub":
		if tok := c.Publish(f.Subject, 1, false, f.Msg); tok.Wait() && tok.Error() != nil {
			log.Fatalf("publish: %v", tok.Error())
		}
		fmt.Printf("published to %q: %s\n", f.Subject, f.Msg)
	case "sub":
		if tok := c.Subscribe(f.Subject, 1, func(_ mqtt.Client, m mqtt.Message) {
			fmt.Printf("%s  [%s] %s\n", time.Now().Format(time.RFC3339), m.Topic(), string(m.Payload()))
		}); tok.Wait() && tok.Error() != nil {
			log.Fatalf("subscribe: %v", tok.Error())
		}
		fmt.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)\n", f.Subject, f.Addr)
		waitForSignal()
	}
}

func waitForSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
