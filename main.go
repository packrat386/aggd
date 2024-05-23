package main

import (
	"log/slog"
	"net"
	"os"
	"strings"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Debug("starting listener")

	laddr, err := net.ResolveUDPAddr("udp", os.Getenv("LISTENER_ADDR"))
	if err != nil {
		panic(err)
	}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	for {
		buf := make([]byte, 1024)

		n, err := conn.Read(buf)
		if err != nil {
			panic(err)
		}

		data := strings.Trim(string(buf), "\x00")

		slog.Debug("received data", "n", n, "data", data)

		lines := strings.Split(data, "\n")

		for _, l := range lines {
			if l == "" {
				continue
			}

			om, err := ParseObservedMetric(l)
			if err != nil {
				panic(err)
			}

			slog.Debug("parsed ObservedMetric", "path", om.Path, "value", om.Value, "timestamp", om.Timestamp)
		}
	}
}
