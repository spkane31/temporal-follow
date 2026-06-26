.PHONY: build test lint install clean

build:
	go build -o bin/temporal-follow ./cmd/temporal-follow

test:
	go test ./...

lint:
	gofmt -l .
	go vet ./...
	golangci-lint run ./...

install:
	go install ./cmd/temporal-follow

clean:
	rm -rf bin
