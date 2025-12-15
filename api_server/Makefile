.PHONY: up down build logs restart

up:
	docker compose --env-file .env up -d --build

down:
	docker compose --env-file .env down

build:
	docker compose --env-file .env build --no-cache

logs:
	docker compose --env-file .env logs -f --tail=50

restart:
	docker compose --env-file .env down && docker compose --env-file .env up -d --build

run-socius:
	docker compose --env-file .env up -d --build zookeeper kafka init-kafka postgres-socius socius-api answer-consumer gateway

run-private:
	docker compose --env-file .env up -d --build zookeeper kafka init-kafka postgres-private private-api answer-consumer gateway

run-ohp:
	docker compose --env-file .env up -d --build zookeeper kafka init-kafka postgres-oakhillpines oakhillpines-api answer-consumer gateway

stop:
	docker compose --env-file .env down
