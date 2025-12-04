# Load env if exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: help run infra stop clean logs enter-db

# Default number of games
N ?= 100

help:
	@echo "Commands:"
	@echo "  make run N=100      # run scraper for top N games (default: 100)"
	@echo "  make infra         # start containers WITHOUT running scraper"
	@echo "  make stop          # stop containers"
	@echo "  make clean         # remove all containers & volumes"
	@echo "  make logs          # show logs"
	@echo "  make enter-db      # enter postgres"

# Run scraper with infrastructure
run:
	@echo "Starting scraper for top $(N) games..."
	@NUMBER_OF_GAMES=$(N) NO_SCRAPER=0 docker-compose up --build

# Start infrastructure only
infra:
	@echo "Starting infrastructure (Postgres)..."
	@NO_SCRAPER=1 docker-compose up -d postgres

stop:
	@docker-compose down

clean:
	@docker-compose down -v

logs:
	@docker-compose logs -f

enter-db:
	@docker exec -it bgg_postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
