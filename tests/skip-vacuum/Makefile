DOCKER ?= docker
PGVERSION ?= 18

test:
	$(DOCKER) compose down
	$(DOCKER) volume rm skip-vacuum 2>/dev/null || true
	$(DOCKER) compose build
	$(DOCKER) compose run test
	$(DOCKER) compose down
	$(DOCKER) volume rm skip-vacuum 2>/dev/null || true

.PHONY: test
