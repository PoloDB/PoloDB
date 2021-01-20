ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))

build:
	cd lib/polodb && cargo build --release
	cp ../target/release/libpolodb.dylib lib/
	go build -ldflags="-r $(ROOT_DIR)lib" main.go

run: build
	./main
