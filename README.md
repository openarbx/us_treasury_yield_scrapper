# US Treasury Yield Scraper

This project ingests historical US Treasury interest-rate datasets from the Treasury XML feed, normalizes the records, stores local CSV/Parquet copies, and upserts the same data into MongoDB.

The main implementation is in `main.py`. Running the script performs a complete refresh for all configured datasets from each dataset's start year through the current UTC year.

## What This Scraper Collects

The pipeline is configured to collect five Treasury datasets:

| Dataset | Treasury `data` key | Local dataset folder | Start year |
| --- | --- | --- | --- |
| Daily Treasury Par Yield Curve Rates | `daily_treasury_yield_curve` | `daily_treasury_par_yield_curve_rates` | 1990 |
| Daily Treasury Bill Rates | `daily_treasury_bill_rates` | `daily_treasury_bill_rates` | 2002 |
| Daily Treasury Long-Term Rates | `daily_treasury_long_term_rate` | `daily_treasury_long_term_rates` | 2000 |
| Daily Treasury Par Real Yield Curve Rates | `daily_treasury_real_yield_curve` | `daily_treasury_real_yield_curve_rates` | 2003 |
| Daily Treasury Real Long-Term Rates | `daily_treasury_real_long_term` | `daily_treasury_real_long_term_rates` | 2000 |

The source endpoint is:

```text
https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml
```

For each dataset and each year, the scraper calls the endpoint with query parameters similar to:

```text
?data=daily_treasury_yield_curve&field_tdr_date_value=2024
```

## How The Pipeline Works

At a high level, `main.py` does the following:

1. Loads environment variables from `.env`.
2. Reads `MONGODB_URI` and optional `MONGODB_DB`.
3. Creates runtime configuration for output folders, retries, timeouts, file formats, MongoDB batch size, and log rotation.
4. Creates a unique `run_id` for the ingestion run.
5. Connects to MongoDB and ensures indexes exist.
6. Loops through all configured Treasury datasets.
7. Downloads XML data year-by-year from the configured start year through the current UTC year.
8. Parses XML `<properties>` records into Python dictionaries.
9. Normalizes dates, numbers, duplicates, and column names.
10. Writes local raw and normalized files.
11. Upserts dataset metadata and daily observations into MongoDB.
12. Writes a run manifest to disk.
13. Marks the MongoDB ingestion run as `ok` or `partial_failure`.

The pipeline is designed so one dataset can fail without stopping the entire run. Dataset failures are recorded in the manifest, logs, and MongoDB `ingestion_runs` collection.

## Project Files

```text
.
├── main.py
├── requirements.txt
├── README.md
└── LICENSE
```

Runtime output is created when the scraper runs:

```text
us_treasury_yields/
├── raw/
├── normalized/
└── metadata/

logs/
└── YYYY-MM-DD/
    ├── 000001.log
    └── 000002.log
```

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create a `.env` file in this folder:

```bash
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB=us_treasury_market_data
```

`MONGODB_URI` is required. `MONGODB_DB` is optional and defaults to `us_treasury_market_data`.

## Running The Scraper

From this folder, run:

```bash
python main.py
```

The script prints the final run manifest as formatted JSON when the run finishes.

Because this is a full historical refresh, the script makes one Treasury request per dataset per year. The first run can take a while, especially when writing Parquet files and upserting all observations into MongoDB.

## Configuration

Most runtime configuration is defined in the `ScraperConfig` dataclass inside `main.py`.

Default settings:

| Setting | Default | Purpose |
| --- | --- | --- |
| `base_dir` | `us_treasury_yields` | Root directory for local data output |
| `logs_dir` | `logs` | Root directory for JSON-line logs |
| `request_timeout` | `60` | HTTP timeout in seconds |
| `max_retries` | `5` | Number of request attempts per year |
| `retry_sleep_seconds` | `1.5` | Base retry backoff multiplier |
| `write_csv` | `True` | Write normalized full-history CSV files |
| `write_parquet` | `True` | Write normalized Parquet files |
| `mongo_batch_size` | `2000` | Number of MongoDB upserts per bulk write |
| `log_lines_per_file` | `5000` | Log rotation threshold per file |

The datasets themselves are configured in `TreasuryYieldScraperPipeline.DEFAULT_DATASETS`.

## Local File Output

For each dataset, the scraper writes both raw-style and normalized outputs under `us_treasury_yields`.

Raw output:

```text
us_treasury_yields/raw/<dataset_id>/
├── full_history.csv
├── 1990.csv
├── 1991.csv
└── ...
```

Normalized output:

```text
us_treasury_yields/normalized/<dataset_id>/
├── full_history.csv
├── full_history.parquet
└── by_year/
    ├── 1990.parquet
    ├── 1991.parquet
    └── ...
```

The "raw" files are written after parsing the XML into tabular rows. The normalized files are written after date parsing, numeric conversion, duplicate removal, and sorting.

The run manifest is written here:

```text
us_treasury_yields/metadata/run_manifest.json
```

The manifest includes:

- `run_id`
- start and finish timestamps
- output directories
- MongoDB database name
- dataset-level row counts
- file paths written
- MongoDB upsert counts
- any dataset errors
- final status

## MongoDB Output

The scraper writes to three MongoDB collections.

### `datasets`

Stores one metadata document per dataset, including:

- `dataset_id`
- display name
- Treasury data key
- source URL
- provider
- columns seen in the latest run
- minimum and maximum record dates
- latest ingestion run ID
- latest row count

The collection has a unique index on `dataset_id`.

### `observations`

Stores one document per dataset per record date.

Each observation includes:

- `dataset_id`
- `record_date`
- `year`, `month`, and `day`
- `source` metadata
- `rates`, a compact dictionary of numeric rate-like fields
- `raw`, the complete normalized row
- ingestion timestamps

The collection has a unique compound index on:

```text
dataset_id + record_date
```

This makes repeated runs idempotent: existing observations are updated, and new observations are inserted.

### `ingestion_runs`

Tracks each pipeline execution by `run_id`.

It records:

- run status
- start and finish timestamps
- requested datasets
- completed datasets
- failed datasets
- error count
- manifest path

## Normalization Details

The normalization step happens in `BaseTreasuryScraper.normalize`.

It performs these operations:

1. Ensures the configured date column exists.
2. Renames known date aliases when needed, such as `index_date` or `quote_date`.
3. Converts the date column to pandas datetime values.
4. Drops rows with invalid or missing dates.
5. Converts object columns to numeric types when every non-null value in that column can be parsed as numeric.
6. Forces configured numeric columns to numeric values.
7. Drops duplicate rows.
8. Sorts the dataset by record date.

The XML parser also normalizes Treasury XML field names to snake case. For example, source tags with mixed case or punctuation are converted into lower-case underscore-separated column names.

## Request And Retry Behavior

The HTTP client uses a shared `requests.Session` with:

- `User-Agent: us-treasury-yield-ingestion/1.0`
- `Accept: application/xml,text/xml,*/*`

Retry behavior:

- `400`, `401`, `403`, and `404` are treated as non-retryable.
- `429`, `500`, `502`, `503`, and `504` are treated as retryable.
- Other HTTP errors use `response.raise_for_status()`.
- Failed attempts sleep for `retry_sleep_seconds * attempt`.
- Empty XML responses are treated as errors.

## Logging

Logs are written as JSON lines to both the console and files.

File logs are stored under:

```text
logs/YYYY-MM-DD/000001.log
```

When a log file reaches `log_lines_per_file`, the handler rotates to the next file:

```text
logs/YYYY-MM-DD/000002.log
```

Each log event includes standard fields such as timestamp, level, module, function, and line number. Pipeline events also include structured fields such as:

- `event`
- `run_id`
- `dataset_id`
- `data_key`
- `year`
- `rows`
- `status_code`
- `elapsed_ms`
- `error_type`

This makes the logs easy to inspect manually or load into a log analysis tool.

## Reading Data Back From MongoDB

`main.py` includes `TreasuryYieldRepository`, a small read-side helper for querying stored data.

Example usage:

```python
from datetime import datetime, timezone

from main import MongoConfig, TreasuryYieldRepository

repo = TreasuryYieldRepository(
    MongoConfig(uri="mongodb://localhost:27017")
)

try:
    metadata = repo.get_dataset_metadata(
        "daily_treasury_par_yield_curve_rates"
    )

    rates = repo.get_rate_matrix(
        "daily_treasury_par_yield_curve_rates",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
    )

    latest_run = repo.get_latest_run()
finally:
    repo.close()
```

Useful repository methods:

| Method | What it returns |
| --- | --- |
| `get_dataset_metadata(dataset_id)` | Metadata document for one dataset |
| `get_history(dataset_id, start_date=None, end_date=None)` | Full observation documents as a DataFrame |
| `get_rate_matrix(dataset_id, start_date=None, end_date=None)` | Date plus flattened `rates` fields as a DataFrame |
| `get_latest_run()` | Most recent ingestion run document |

## Idempotency

The scraper is safe to run repeatedly against the same MongoDB database.

MongoDB writes use upserts keyed by:

```text
dataset_id + record_date
```

That means a later run updates existing records with the latest parsed source values instead of inserting duplicate daily observations.

Local files are overwritten at stable paths such as `full_history.csv`, `full_history.parquet`, and per-year files.

## Failure Handling

A failure in one dataset does not immediately stop the full pipeline.

If one dataset fails:

- the error is added to `manifest["errors"]`
- the dataset is marked as failed in MongoDB
- the error is written to logs with exception details
- the pipeline continues with the next dataset
- the final run status becomes `partial_failure`

If all datasets complete successfully, the final status is `ok`.

## Notes And Limitations

- The script performs a full historical refresh each time it runs.
- There is no command-line argument parser yet; configuration is currently edited in `main.py`.
- MongoDB is required for the current entrypoint.
- Parquet output requires `pyarrow`, which is listed in `requirements.txt`.
- The local folder name uses `scrapper`; this README keeps the existing project name, although the common spelling is `scraper`.
