// mqttcli — a minimal MQTT pub/sub client for the Mosquitto bridge cluster.
//
//	mqttcli pub -addr 127.0.0.1:30883 -subject demo/topic -msg "hello"
//	mqttcli pub -subject demo/topic -count 100 -rate 10/s
//	mqttcli sub -subject demo/topic -count 5 -timeout 10s -json
//
// The brokers allow anonymous connections (lab config). Because the three
// Mosquitto pods are full-mesh bridged, a publish that lands on any broker
// is delivered to subscribers connected to the others.
package main

import (
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/cli"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/metrics"
)

func main() {
	f := cli.Parse("mqttcli", "127.0.0.1:30883", "")

	// MQTT has no HA flag here — the brokers are full-mesh bridged, so a single
	// "bridge" mode covers it.
	rec, err := metrics.Setup(metrics.Config{Addr: f.MetricsAddr, Bus: "mqtt", Mode: "bridge", Role: f.Cmd})
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + f.Addr).
		SetClientID(fmt.Sprintf("mqttcli-%d", os.Getpid())).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			rec.IncReconnect()
			log.Printf("connection lost: %v", err)
		})
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalf("connect: %v", tok.Error())
	}
	defer c.Disconnect(250)

	switch f.Cmd {
	case "pub":
		if err := f.PubLoop(func(body string) error {
			tok := c.Publish(f.Subject, 1, false, body)
			tok.Wait()
			if tok.Error() != nil {
				rec.IncPublishError()
				return tok.Error()
			}
			rec.IncPublished()
			return nil
		}); err != nil {
			log.Fatalf("%v", err)
		}
	case "sub":
		lim := f.NewLimiter()
		var gaps metrics.GapTracker
		if tok := c.Subscribe(f.Subject, 1, func(_ mqtt.Client, m mqtt.Message) {
			metrics.RecordReceived(rec, &gaps, string(m.Payload()))
			f.Emit(m.Topic(), string(m.Payload()))
			lim.Hit()
		}); tok.Wait() && tok.Error() != nil {
			log.Fatalf("subscribe: %v", tok.Error())
		}
		log.Printf("subscribed to %q on %s; waiting for messages (Ctrl-C to quit)", f.Subject, f.Addr)
		select {
		case <-f.Stop():
		case <-lim.Done():
		}
	}
}
