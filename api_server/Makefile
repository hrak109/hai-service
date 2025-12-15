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
