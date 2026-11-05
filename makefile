.PHONY: up down produce consume logs bash

up:
	docker compose up -d

down:
	docker compose down

produce:
	python3 src/producer.py

consume:
	python3 src/consumer.py

logs:
	docker compose logs -f

bash:
	docker compose exec -it kafka bash
