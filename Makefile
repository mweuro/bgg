# Load environment variables from .env file if it exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Phony targets
.PHONY: help run stop clean logs enter-db


#  HELP COMMAND

help:
	@echo "BGG Scraper - Docker Compose Management"
	@echo ""
	@echo "Usage:"
	@echo "  make run [N=number]    Start scraper for top N games (default: 50)"
	@echo "  make stop              Stop all services"
	@echo "  make clean             Stop and remove all containers and volumes"
	@echo "  make logs              Show logs from running services"
	@echo "  make help              Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make run           # Extract top 50 games"
	@echo "  make run N=20      # Extract top 20 games"
	@echo "  make run N=100     # Extract top 100 games"


# Default arguments
N ?= 50
DB_USER ?= myuser
DB_NAME ?= bgg


#  CONTAINER MANAGEMENT COMMANDS

run:
	@echo "Starting BGG scraper for top $(N) games..."
	@echo "This might take a while..."
	@NUMBER_OF_GAMES=$(N) docker-compose up --build

stop:
	@echo "Stopping services..."
	@docker-compose down

clean:
	@echo "Cleaning up containers and volumes..."
	@docker-compose down -v

logs:
	@docker-compose logs -f



# DATABASE MANAGEMENT COMMANDS

enter-db:
	@echo "Entering PostgreSQL database container..."
	@docker exec -it bgg_postgres psql -U $(DB_USER) -d $(DB_NAME)