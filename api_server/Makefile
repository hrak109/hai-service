.PHONY: up down build logs restart

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build --no-cache

logs:
	docker compose logs -f --tail=50

restart:
	docker compose down && docker compose up -d --build
