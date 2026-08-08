.PHONY: build test vet run docker

build:
	go build -o bin/server ./cmd/server

test:
	go test ./...

vet:
	go vet ./...

run:
	go run ./cmd/server

docker:
	docker build -t order-service:dev .
