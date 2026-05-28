# Warehouse and Retail Sales Analysis

This project analyzes the `Warehouse_and_Retail_Sales.csv` dataset with Apache Spark, cleans and normalizes the data, then produces summary metrics and a chart of the top suppliers by retail sales.

## What the project does

Running the analysis will:

1. Load the CSV file from `src/database/Warehouse_and_Retail_Sales.csv`.
2. Normalize column names, whitespace, casing, and date fields.
3. Remove invalid rows and add a `total_distribution_sales` column.
4. Print summary statistics in the console.
5. Save a cleaned CSV file to `src/outputs/cleaned.csv`.
6. Save a bar chart to `src/outputs/top_suppliers_sales.png`.

## Project structure

```text
.
├── Dockerfile
├── docker-compose.yml
├── main.py
├── pyproject.toml
├── src/
│   ├── app/
│   │   └── sale_analysis.py
│   ├── database/
│   │   └── Warehouse_and_Retail_Sales.csv
│   └── outputs/           # created when the analysis runs
```

## Requirements

- Docker and Docker Compose
- About 4 GB of free memory for Spark, depending on your dataset size

If you want to run the Python code outside Docker, you will also need:

- Python 3.12 or newer
- Java 17
- Apache Spark available locally

## Setup with Docker

The easiest way to run the project is with Docker Compose.

```bash
docker compose build
docker compose up -d
```

This starts two containers:

- `spark-master` on ports `7077` and `8080`
- `spark-worker` on port `8081`

## Run the analysis

The container-accessible analysis script is `src/app/sale_analysis.py`, mounted in the Spark container at `/opt/spark-apps/app/sale_analysis.py`. It creates a `WarehouseRetailSalesAnalysis` instance and connects to the Spark master at `spark://spark-master:7077`.

Run it inside the Spark container environment, for example:

```bash
docker compose exec spark-master python /opt/spark-apps/app/sale_analysis.py
```

or

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/app/sale_analysis.py
```

If you prefer to run it from the host, the script must still be able to reach a Spark master at `spark://spark-master:7077`.

## Expected output

During execution, the script prints:

- a sample of the raw data
- the Spark schema
- normalized data preview
- cleaned data preview
- cleaning summary
- total retail sales
- average retail sales
- sales by item type
- the top supplier by retail sales

It also writes these files:

- `src/outputs/cleaned.csv`
- `src/outputs/top_suppliers_sales.png`

## Usage details

The main analysis logic lives in [src/app/sale_analysis.py](src/app/sale_analysis.py).

Key methods:

- `load_data()` reads the CSV with a defined schema.
- `normalize_data()` standardizes text and derives `year_month` and `sale_date`.
- `clean_data()` removes invalid rows and computes `total_distribution_sales`.
- `save_cleaned_data()` exports the cleaned dataset.
- `run_analysis()` executes the full pipeline in order.

## Customization

You can extend the analysis by editing [src/app/sale_analysis.py](src/app/sale_analysis.py) and adding new aggregation or visualization methods. Common changes include:

- adjusting the cleaning rules
- exporting additional summary files
- generating more charts
- changing the output directory

## Troubleshooting

- If Spark cannot connect, make sure the `spark-master` container is running.
- If the analysis fails to start, verify that the container has access to the CSV file mounted from `src/database`.
- If charts are not generated, check that `matplotlib` is installed inside the environment you are using.

## Stop the cluster

```bash
docker compose down
```

Use `docker compose down -v` if you also want to remove volumes.
