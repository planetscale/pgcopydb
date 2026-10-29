# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

TOP := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PGCOPYDB ?= $(TOP)src/bin/pgcopydb/pgcopydb
PGVERSION ?= 18
DOCKER ?= docker

MARKDOWNLINT ?= davidanson/markdownlint-cli2@sha256:9ae6011b3d978315ad283bf2af997ef08687a797b9e55086c94a90bf620f0783

all: bin ;

GIT-VERSION-FILE:
	@$(SHELL_PATH) ./GIT-VERSION-GEN > /dev/null 2>&1

bin: GIT-VERSION-FILE
	$(MAKE) -C src/bin/ all

sqlite3:
	$(MAKE) -C src/bin/lib/sqlite $@

clean:
	rm -f GIT-VERSION-FILE
	$(MAKE) -C src/bin/ clean

maintainer-clean:
	rm -f GIT-VERSION-FILE
	$(MAKE) -C src/bin/ maintainer-clean
	rm -f version

update-docs: bin
	bash ./docs/update-help-messages.sh

check-docs:
	cat Dockerfile ci/Dockerfile.docs.template > ci/Dockerfile.docs
	$(DOCKER) build --file=ci/Dockerfile.docs --tag test-docs .
	$(DOCKER) run test-docs

lint-docs:
	$(DOCKER) run --rm -v "$(CURDIR)":/workdir $(MARKDOWNLINT)

release-notes:
	@bash ./ci/release-notes.sh $(TAG)

test: build
	$(MAKE) -C tests all

tests: test ;

tests/ci:
	sh ./ci/banned.h.sh

tests/*: build
	$(MAKE) -C tests $(notdir $@)

install: bin
	$(MAKE) -C src/bin/ install

indent:
	citus_indent

build: version
	$(DOCKER) build --build-arg PGVERSION=$(PGVERSION) -t pgcopydb .

echo-version: GIT-VERSION-FILE
	@awk '{print $$3}' $<

version: GIT-VERSION-FILE
	@awk '{print $$3}' $< > $@
	@cat $@

# debian packages built from the current sources
deb:
	$(DOCKER) build -f Dockerfile.debian -t pgcopydb_debian .

debsh: deb
	$(DOCKER) run --rm -it pgcopydb_debian bash

# debian packages built from latest tag, manually maintained in the Dockerfile
deb-qa:
	$(DOCKER) build -f Dockerfile.debian-qa -t pgcopydb_debian_qa .

debsh-qa: deb-qa
	$(DOCKER) run --rm -it pgcopydb_debian_qa bash

.PHONY: all
.PHONY: bin clean install maintainer-clean update-docs lint-docs release-notes
.PHONY: test tests tests/ci tests/*
.PHONY: deb debsh deb-qa debsh-qa
.PHONY: GIT-VERSION-FILE
