# HINT
# Use this file to override variables here.
# For example, to run with podman put `DOCKER=podman` there.
-include Makefile.env

# NOTE
# Variables specified in `.env` file are used to pick and setup specific
# component versions, both when building a development image and when running
# CI workflows on GH Actions. This ensures that tasks run with `wc-` prefix
# (like `wc-dialyze`) are reproducible between local machine and CI runners.
DOTENV := $(shell grep -v '^\#' .env)

# Development images
DEV_IMAGE_TAG = $(TEST_CONTAINER_NAME)-dev
DEV_IMAGE_ID = $(file < .image.dev)

TEST_IMAGE_TAG = $(DIST_CONTAINER_NAME)-test
TEST_IMAGE_ID = $(file < .image.test)

DOCKER ?= docker
DOCKERCOMPOSE ?= docker-compose
DOCKERCOMPOSE_W_ENV = DEV_IMAGE_TAG=$(DEV_IMAGE_TAG) $(DOCKERCOMPOSE) -f compose.yaml -f compose.tracing.yaml
REBAR ?= rebar3
TEST_CONTAINER_NAME ?= testrunner
DIST_CONTAINER_NAME ?= distrunner

all: compile xref lint check-format dialyze eunit

.PHONY: dev-image test-image clean-dev-image clean-test-image wc-shell test

dev-image: .image.dev

.image.dev: Dockerfile.dev .env
	env $(DOTENV) $(DOCKERCOMPOSE_W_ENV) build $(TEST_CONTAINER_NAME)
	$(DOCKER) image ls -q -f "reference=$(DEV_IMAGE_ID)" | head -n1 > $@

clean-dev-image:
ifneq ($(DEV_IMAGE_ID),)
	$(DOCKER) image rm -f $(DEV_IMAGE_TAG)
	rm .image.dev
endif

test-image: .image.test

.image.test: Dockerfile.neighbour .env
	env $(DOTENV) $(DOCKERCOMPOSE_W_ENV) build $(DIST_CONTAINER_NAME)
	$(DOCKER) image ls -q -f "reference=$(TEST_IMAGE_ID)" | head -n1 > $@

clean-test-image:
ifneq ($(TEST_IMAGE_ID),)
	$(DOCKER) image rm -f $(TEST_IMAGE_TAG)
	rm .image.test
endif

DOCKER_WC_OPTIONS := -v $(PWD):$(PWD) --workdir $(PWD)
DOCKER_WC_EXTRA_OPTIONS ?= --rm
DOCKER_RUN = $(DOCKER) run -t $(DOCKER_WC_OPTIONS) $(DOCKER_WC_EXTRA_OPTIONS)

DOCKERCOMPOSE_RUN = $(DOCKERCOMPOSE_W_ENV) run --rm $(DOCKER_WC_OPTIONS) $(TEST_CONTAINER_NAME)

# Utility tasks

wc-shell: dev-image
	$(DOCKER_RUN) --interactive --tty $(DEV_IMAGE_TAG)

wc-%: dev-image
	$(DOCKER_RUN) $(DEV_IMAGE_TAG) make $*

wdeps-shell: dev-image
	$(DOCKERCOMPOSE_RUN) su; \
	$(DOCKERCOMPOSE_W_ENV) down

wdeps-%: dev-image
	$(DOCKERCOMPOSE_RUN) make $*; \
	res=$$?; \
	$(DOCKERCOMPOSE_W_ENV) down; \
	exit $$res

# Rebar tasks

rebar-shell:
	$(REBAR) shell

compile:
	$(REBAR) compile

xref:
	$(REBAR) xref

lint:
	$(REBAR) lint

check-format:
	$(REBAR) fmt -c

dialyze:
	$(REBAR) as test dialyzer

release:
	$(REBAR) as prod release

eunit:
	$(REBAR) eunit --cover

common-test:
	$(REBAR) ct --cover --name test_node@127.0.0.1

cover:
	$(REBAR) covertool generate

format:
	$(REBAR) fmt -w

clean:
	$(REBAR) clean

distclean: clean-dev-image
	rm -rf _build

test: test-configurator eunit common-test

cover-report:
	$(REBAR) cover

test-configurator:
	ERL_LIBS=_build/default/lib ./rel_scripts/configurator.escript config/config.yaml config
