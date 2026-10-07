.PHONY: up deps down fmt vet test test-race test-integration test-integration-race migrate-up migrate-down multi logs

DATABASE_URL ?= postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable

up:                 ## full stack (app + dependencies)
	docker compose up --build

deps:               ## only PostgreSQL, Keycloak and LocalStack (for integration tests / local go run)
	docker compose up -d --wait postgres keycloak localstack

multi:              ## three app instances (:8080, :8090, :8091)
	docker compose --profile multi up --build

down:
	docker compose --profile multi down -v

fmt:
	gofmt -l -w .

vet:
	go vet ./...
	go vet -tags integration ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:   ## requires `make deps`
	go test -tags integration -count=1 -p 1 -timeout 15m ./test/integration/...

test-integration-race:
	go test -race -tags integration -count=1 -p 1 -timeout 20m ./test/integration/...

migrate-up:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/migrate up

migrate-down:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/migrate down 1

logs:
	docker compose logs -f app
