# Load env
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: help run infra stop clean logs enter-db

N ?= 50

help:
	@echo "Commands:"
	@echo "  make run N=50      # run scraper for top N games"
	@echo "  make infra         # start containers WITHOUT running scraper"
	@echo "  make stop          # stop containers"
	@echo "  make clean         # remove all containers & volumes"
	@echo "  make logs          # show logs"
	@echo "  make enter-db      # enter postgres"

run:
	@echo "Starting scraper for top $(N) games..."
	@NUMBER_OF_GAMES=$(N) NO_SCRAPER=0 docker-compose up --build

infra:
	@echo "Starting infrastructure WITHOUT scraper..."
	@NO_SCRAPER=1 docker-compose up -d

stop:
	@docker-compose down

clean:
	@docker-compose down -v

logs:
	@docker-compose logs -f

enter-db:
	@docker exec -it bgg_postgres psql -U $(DB_USER) -d $(DB_NAME)
