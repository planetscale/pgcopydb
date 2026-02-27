# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

COMPOSE_EXIT = --exit-code-from=test --abort-on-container-exit

test: down run down ;

up: down build
	$(DOCKER) compose up $(COMPOSE_EXIT)

run: build
	$(DOCKER) compose run test

down:
	$(DOCKER) compose down

build:
	$(DOCKER) compose build

.PHONY: run down build test
