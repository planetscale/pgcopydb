DOCKER ?= docker
PGVERSION ?= 17

test:
	$(DOCKER) compose down
	$(DOCKER) volume rm skip-large-objects 2>/dev/null || true
	$(DOCKER) compose build
	$(DOCKER) compose run test
	$(DOCKER) compose down
	$(DOCKER) volume rm skip-large-objects 2>/dev/null || true

.PHONY: test
