build:
	go build .

run: build
	LISTENER_ADDR=127.0.0.1:8125 ./aggd

clean:
	rm aggd
