SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	mkdir -p output
	rm ./output/* -f
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	PYTHONPATH="$(PWD)/src/common" python3 ./verify_output.py
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: test

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada filtro de la query 1"
	@echo "2) Un cliente, una sola réplica de cada elemento de la query 2"
	@echo "3) Un cliente, una sola réplica de cada elemento de la query 3"
	@echo "4) Un cliente, una sola réplica de cada elemento de la query 4"
	@echo "5) Un cliente, una sola réplica de cada elemento de la query 5"
	@read -p "Selecciona uno [1-5]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch