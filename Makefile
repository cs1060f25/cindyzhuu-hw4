DB=data.db
FUNC_DB=netlify/functions/county_data/data.db

build-db:
	python3 csv_to_sqlite.py $(DB) zip_county.csv
	python3 csv_to_sqlite.py $(DB) county_health_rankings.csv

deploy-db: build-db
	mkdir -p netlify/functions/county_data
	cp $(DB) $(FUNC_DB)

update: build-db deploy-db
	@echo "Database rebuilt and copied to function folder."
