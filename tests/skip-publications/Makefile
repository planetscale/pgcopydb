DOCKER ?= docker
PGVERSION ?= 16

test:
	$(DOCKER) compose down
	$(DOCKER) volume rm skip-publications 2>/dev/null || true
	$(DOCKER) compose build
	$(DOCKER) compose run test
	$(DOCKER) compose down
	$(DOCKER) volume rm skip-publications 2>/dev/null || true

.PHONY: test
