.PHONY: help build run shell

help:
	@echo " build Builds the docker images for the docker-compose setup."
	@echo " run   Runs a command."
	@echo " shell Opens a bash shell

build:
	docker-compose build

run:
	docker-compose run app $(COMMAND)

shell:
	docker-compose run --entrypoint /bin/bash app
