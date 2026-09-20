// sysexporter — a tiny Mosquitto $SYS → Prometheus bridge (design §11.5).
//
// Mosquitto has no native Prometheus endpoint, so this sidecar subscribes to
// the broker's $SYS/# tree (republished every sys_interval, default 10s) and
// mirrors the useful counters/gauges as mosquitto_* metrics. One runs per
// broker pod and is scraped per-pod, exactly like the NATS/redis exporter
// sidecars — $SYS is broker-local (never carried over the bridge mesh), so each
// exporter sees only its own pod's stats.
//
//	sysexporter -addr localhost:1883 -metrics-addr :9234
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var version = "dev" // stamped via -ldflags -X main.version (nix/lib/mkGoBinary.nix)

// counter mirrors a broker-reported cumulative total as a real Prometheus
// counter. Mosquitto resets its $SYS totals to 0 on restart, so we add the
// delta between samples and treat any decrease as a reset (add the new value).
type counter struct {
	c    prometheus.Counter
	last float64
	seen bool
}

func (m *counter) set(v float64) {
	switch {
	case !m.seen, v < m.last:
		m.c.Add(v) // first sample, or a post-restart reset
	default:
		m.c.Add(v - m.last)
	}
	m.last, m.seen = v, true
}

func main() {
	addr := flag.String("addr", "localhost:1883", "mosquitto broker host:port")
	metricsAddr := flag.String("metrics-addr", ":9234", "host:port to serve /metrics on")
	flag.Parse()

	reg := prometheus.NewRegistry()
	newCounter := func(name, help string) *counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		reg.MustRegister(c)
		return &counter{c: c}
	}
	newGauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		reg.MustRegister(g)
		return g
	}

	// $SYS topic → the metric it feeds. Counters mirror cumulative broker
	// totals; gauges mirror point-in-time values (design §11.5).
	msgRecv := newCounter("mosquitto_messages_received_total", "MQTT messages received by the broker")
	msgSent := newCounter("mosquitto_messages_sent_total", "MQTT messages sent by the broker")
	bytesRecv := newCounter("mosquitto_bytes_received_total", "bytes received by the broker")
	bytesSent := newCounter("mosquitto_bytes_sent_total", "bytes sent by the broker")
	clientsConn := newGauge("mosquitto_clients_connected", "currently connected clients")
	load1min := newGauge("mosquitto_load_messages_received_1min", "1-min moving average of messages received per second")
	heap := newGauge("mosquitto_heap_current_bytes", "current heap memory in use, bytes")

	handlers := map[string]func(float64){
		"$SYS/broker/messages/received":           msgRecv.set,
		"$SYS/broker/messages/sent":               msgSent.set,
		"$SYS/broker/bytes/received":              bytesRecv.set,
		"$SYS/broker/bytes/sent":                  bytesSent.set,
		"$SYS/broker/clients/connected":           clientsConn.Set,
		"$SYS/broker/load/messages/received/1min": load1min.Set,
		"$SYS/broker/heap/current":                heap.Set,
	}

	// Serialize handler updates: the counter delta logic reads+writes its own
	// state, and paho may deliver concurrently. $SYS traffic is ~7 msgs / 10s,
	// so a single mutex is ample.
	var mu sync.Mutex
	onMsg := func(_ mqtt.Client, m mqtt.Message) {
		h, ok := handlers[m.Topic()]
		if !ok {
			return
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(string(m.Payload())), 64)
		if err != nil {
			return // some $SYS values are non-numeric (version, timestamp); skip
		}
		mu.Lock()
		h(v)
		mu.Unlock()
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + *addr).
		SetClientID(fmt.Sprintf("mosquitto-sysexporter-%d", os.Getpid())).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		// (Re)subscribe on every (re)connect — a dropped session loses the sub.
		SetOnConnectHandler(func(c mqtt.Client) {
			if tok := c.Subscribe("$SYS/#", 0, onMsg); tok.Wait() && tok.Error() != nil {
				log.Printf("subscribe: %v", tok.Error())
			}
		})
	c := mqtt.NewClient(opts)
	// ConnectRetry keeps retrying in the background, so a failed first attempt
	// is not fatal — the exporter still serves /metrics (empty until connected).
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Printf("connect (will retry): %v", tok.Error())
	}
	defer c.Disconnect(250)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("mosquitto-sysexporter %s: broker=%s metrics=%s", version, *addr, *metricsAddr)
	log.Fatal(srv.ListenAndServe())
}
