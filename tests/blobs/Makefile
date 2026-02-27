# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

test: down run down ;

run: build
	$(DOCKER) compose run test

down:
	$(DOCKER) compose down

build:
	$(DOCKER) compose build

import:
	psql --single-transaction --no-psqlrc -f import.sql


.PHONY: run down build test import
