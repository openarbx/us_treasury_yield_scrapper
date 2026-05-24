from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
import xml.etree.ElementTree as ET

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError, PyMongoError


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class DatasetConfig:
    name: str
    data_key: str
    folder_name: str
    start_year: int
    date_column: str = "record_date"
    date_column_aliases: Tuple[str, ...] = ()
    numeric_columns: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ScraperConfig:
    base_dir: Path = Path("us_treasury_yields")
    logs_dir: Path = Path("logs")

    request_timeout: int = 60
    max_retries: int = 5
    retry_sleep_seconds: float = 1.5

    write_csv: bool = True
    write_parquet: bool = True

    mongo_batch_size: int = 2_000
    log_lines_per_file: int = 5_000


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    database: str = "us_treasury_market_data"


# =============================================================================
# Exceptions
# =============================================================================

class NonRetryableSourceError(RuntimeError):
    pass


# =============================================================================
# Logging
# =============================================================================

class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        reserved = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message",
        }

        for key, value in record.__dict__.items():
            if key not in reserved:
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


class LineCountDailyRotatingFileHandler(logging.Handler):
    """
    Log path:

        logs/YYYY-MM-DD/000001.log
        logs/YYYY-MM-DD/000002.log
        ...

    Rotation rule:

        after 5000 lines, open the next id file in the same date folder.
    """

    def __init__(
        self,
        base_dir: Path,
        max_lines: int = 5_000,
        encoding: str = "utf-8",
    ):
        super().__init__()
        self.base_dir = Path(base_dir)
        self.max_lines = int(max_lines)
        self.encoding = encoding

        self.current_date: Optional[str] = None
        self.current_log_id: Optional[int] = None
        self.current_line_count: int = 0
        self.stream = None

        self.createLock()
        self._open_current_stream()

    @staticmethod
    def _today_str() -> str:
        return date.today().isoformat()

    def _date_dir(self, day: str) -> Path:
        path = self.base_dir / day
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _count_lines(self, path: Path) -> int:
        if not path.exists():
            return 0

        count = 0
        with path.open("r", encoding=self.encoding, errors="ignore") as f:
            for _ in f:
                count += 1

        return count

    def _existing_log_files(self, day_dir: Path) -> List[Path]:
        files: List[Path] = []

        for path in day_dir.glob("*.log"):
            try:
                int(path.stem)
                files.append(path)
            except ValueError:
                continue

        return sorted(files, key=lambda p: int(p.stem))

    def _select_log_file(self, day: str) -> Tuple[int, int, Path]:
        day_dir = self._date_dir(day)
        files = self._existing_log_files(day_dir)

        if not files:
            log_id = 1
            path = day_dir / f"{log_id:06d}.log"
            return log_id, 0, path

        latest = files[-1]
        latest_id = int(latest.stem)
        latest_line_count = self._count_lines(latest)

        if latest_line_count >= self.max_lines:
            next_id = latest_id + 1
            path = day_dir / f"{next_id:06d}.log"
            return next_id, 0, path

        return latest_id, latest_line_count, latest

    def _open_current_stream(self) -> None:
        day = self._today_str()

        if self.stream:
            self.stream.flush()
            self.stream.close()

        log_id, line_count, path = self._select_log_file(day)

        self.current_date = day
        self.current_log_id = log_id
        self.current_line_count = line_count
        self.stream = path.open("a", encoding=self.encoding)

    def _rotate_if_needed(self) -> None:
        day = self._today_str()

        if day != self.current_date:
            self._open_current_stream()
            return

        if self.current_line_count < self.max_lines:
            return

        if self.stream:
            self.stream.flush()
            self.stream.close()

        assert self.current_date is not None
        assert self.current_log_id is not None

        self.current_log_id += 1
        self.current_line_count = 0

        path = self._date_dir(self.current_date) / f"{self.current_log_id:06d}.log"
        self.stream = path.open("a", encoding=self.encoding)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)

            with self.lock:
                self._rotate_if_needed()

                assert self.stream is not None
                self.stream.write(msg + "\n")
                self.stream.flush()
                self.current_line_count += 1

        except Exception:
            self.handleError(record)

    def close(self) -> None:
        with self.lock:
            if self.stream:
                self.stream.flush()
                self.stream.close()
                self.stream = None

        super().close()


def build_logger(config: ScraperConfig) -> logging.Logger:
    logger = logging.getLogger("TreasuryYieldPipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    formatter = JsonLineFormatter()

    file_handler = LineCountDailyRotatingFileHandler(
        base_dir=config.logs_dir,
        max_lines=config.log_lines_per_file,
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# =============================================================================
# Treasury XML Client
# =============================================================================

class TreasuryXmlClient:
    BASE_URL = (
        "https://home.treasury.gov/resource-center/"
        "data-chart-center/interest-rates/pages/xml"
    )

    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404}
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self, config: ScraperConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "us-treasury-yield-ingestion/1.0",
                "Accept": "application/xml,text/xml,*/*",
            }
        )

    @staticmethod
    def _local_name(tag: str) -> str:
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag

    @staticmethod
    def _snake_case(name: str) -> str:
        name = name.strip()
        name = re.sub(r"[^A-Za-z0-9]+", "_", name)
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.strip("_").lower()

    @staticmethod
    def _parse_scalar(value: Optional[str]) -> Any:
        if value is None:
            return None

        value = value.strip()

        if value == "":
            return None

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", value):
            return value[:10]

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return value

        try:
            if re.fullmatch(r"[-+]?\d+", value):
                return int(value)

            if re.fullmatch(r"[-+]?\d*\.\d+", value):
                return float(value)
        except ValueError:
            return value

        return value

    def _request_year_xml(
        self,
        *,
        data_key: str,
        year: int,
        run_id: str,
        dataset_id: str,
    ) -> str:
        params = {
            "data": data_key,
            "field_tdr_date_value": str(year),
        }

        last_exception: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            started = time.perf_counter()

            try:
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.config.request_timeout,
                )

                elapsed_ms = round((time.perf_counter() - started) * 1000, 3)

                self.logger.info(
                    "Treasury XML request completed",
                    extra={
                        "event": "treasury_xml_request_completed",
                        "run_id": run_id,
                        "dataset_id": dataset_id,
                        "data_key": data_key,
                        "year": year,
                        "url": response.url,
                        "status_code": response.status_code,
                        "elapsed_ms": elapsed_ms,
                        "attempt": attempt,
                    },
                )

                if response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                    raise NonRetryableSourceError(
                        f"Non-retryable HTTP {response.status_code}: {response.url}"
                    )

                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    raise requests.HTTPError(
                        f"Retryable HTTP {response.status_code}: {response.text[:500]}"
                    )

                response.raise_for_status()

                text = response.text.strip()

                if not text:
                    raise RuntimeError(f"Empty XML response: {response.url}")

                return text

            except NonRetryableSourceError:
                self.logger.error(
                    "Treasury XML non-retryable error",
                    extra={
                        "event": "treasury_xml_non_retryable_error",
                        "run_id": run_id,
                        "dataset_id": dataset_id,
                        "data_key": data_key,
                        "year": year,
                        "params": params,
                    },
                    exc_info=True,
                )
                raise

            except Exception as exc:
                last_exception = exc

                self.logger.warning(
                    "Treasury XML request failed",
                    extra={
                        "event": "treasury_xml_request_failed",
                        "run_id": run_id,
                        "dataset_id": dataset_id,
                        "data_key": data_key,
                        "year": year,
                        "attempt": attempt,
                        "max_retries": self.config.max_retries,
                        "error_type": type(exc).__name__,
                        "error": repr(exc),
                    },
                    exc_info=True,
                )

                time.sleep(self.config.retry_sleep_seconds * attempt)

        raise RuntimeError(f"Treasury XML request failed after retries: {last_exception}")

    def _parse_xml_rows(self, xml_text: str) -> List[Dict[str, Any]]:
        root = ET.fromstring(xml_text)
        rows: List[Dict[str, Any]] = []

        for elem in root.iter():
            if self._local_name(elem.tag) != "properties":
                continue

            row: Dict[str, Any] = {}

            for child in list(elem):
                raw_key = self._local_name(child.tag)
                key = self._snake_case(raw_key)

                null_flag = None
                for attr_key, attr_value in child.attrib.items():
                    if self._local_name(attr_key) == "null":
                        null_flag = attr_value
                        break

                if null_flag == "true":
                    value = None
                else:
                    value = self._parse_scalar(child.text)

                row[key] = value

            if not row:
                continue

            if "new_date" in row and "record_date" not in row:
                row["record_date"] = row.pop("new_date")

            rows.append(row)

        return rows

    def fetch_dataset_history(
        self,
        dataset: DatasetConfig,
        *,
        run_id: str,
    ) -> pd.DataFrame:
        current_year = datetime.now(timezone.utc).year
        dataset_id = dataset.folder_name

        all_rows: List[Dict[str, Any]] = []

        for year in range(dataset.start_year, current_year + 1):
            xml_text = self._request_year_xml(
                data_key=dataset.data_key,
                year=year,
                run_id=run_id,
                dataset_id=dataset_id,
            )

            rows = self._parse_xml_rows(xml_text)

            self.logger.info(
                "Treasury XML year parsed",
                extra={
                    "event": "treasury_xml_year_parsed",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "data_key": dataset.data_key,
                    "year": year,
                    "rows": len(rows),
                },
            )

            all_rows.extend(rows)

        return pd.DataFrame(all_rows)


# =============================================================================
# File Storage
# =============================================================================

class TreasuryFileStorage:
    def __init__(self, base_dir: Path, logger: logging.Logger):
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.normalized_dir = self.base_dir / "normalized"
        self.metadata_dir = self.base_dir / "metadata"
        self.logger = logger

        for path in [self.raw_dir, self.normalized_dir, self.metadata_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def dataset_raw_dir(self, dataset: DatasetConfig) -> Path:
        path = self.raw_dir / dataset.folder_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def dataset_normalized_dir(self, dataset: DatasetConfig) -> Path:
        path = self.normalized_dir / dataset.folder_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_dataframe(
        self,
        df: pd.DataFrame,
        path_without_suffix: Path,
        *,
        write_csv: bool,
        write_parquet: bool,
        run_id: str,
        dataset_id: str,
    ) -> Dict[str, str]:
        written: Dict[str, str] = {}

        if write_csv:
            csv_path = path_without_suffix.with_suffix(".csv")
            df.to_csv(csv_path, index=False)
            written["csv"] = str(csv_path)

            self.logger.info(
                "CSV written",
                extra={
                    "event": "file_written",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "file_type": "csv",
                    "path": str(csv_path),
                    "rows": len(df),
                },
            )

        if write_parquet:
            parquet_path = path_without_suffix.with_suffix(".parquet")
            df.to_parquet(parquet_path, index=False)
            written["parquet"] = str(parquet_path)

            self.logger.info(
                "Parquet written",
                extra={
                    "event": "file_written",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "file_type": "parquet",
                    "path": str(parquet_path),
                    "rows": len(df),
                },
            )

        return written

    def persist_dataset(
        self,
        df: pd.DataFrame,
        dataset: DatasetConfig,
        config: ScraperConfig,
        *,
        run_id: str,
    ) -> Dict[str, Any]:
        dataset_id = dataset.folder_name

        if df.empty:
            return {
                "dataset_id": dataset_id,
                "rows": 0,
                "status": "empty",
            }

        raw_dir = self.dataset_raw_dir(dataset)
        norm_dir = self.dataset_normalized_dir(dataset)
        by_year_dir = norm_dir / "by_year"
        by_year_dir.mkdir(parents=True, exist_ok=True)

        full_raw_paths = self.write_dataframe(
            df,
            raw_dir / "full_history",
            write_csv=True,
            write_parquet=False,
            run_id=run_id,
            dataset_id=dataset_id,
        )

        full_norm_paths = self.write_dataframe(
            df,
            norm_dir / "full_history",
            write_csv=config.write_csv,
            write_parquet=config.write_parquet,
            run_id=run_id,
            dataset_id=dataset_id,
        )

        annual_files: Dict[str, Dict[str, str]] = {}

        for year, year_df in df.groupby(df[dataset.date_column].dt.year):
            year_int = int(year)

            raw_written = self.write_dataframe(
                year_df,
                raw_dir / str(year_int),
                write_csv=True,
                write_parquet=False,
                run_id=run_id,
                dataset_id=dataset_id,
            )

            norm_written = self.write_dataframe(
                year_df,
                by_year_dir / str(year_int),
                write_csv=False,
                write_parquet=config.write_parquet,
                run_id=run_id,
                dataset_id=dataset_id,
            )

            annual_files[str(year_int)] = {
                **{f"raw_{k}": v for k, v in raw_written.items()},
                **{f"normalized_{k}": v for k, v in norm_written.items()},
            }

        return {
            "dataset_id": dataset_id,
            "rows": int(len(df)),
            "min_date": str(df[dataset.date_column].min().date()),
            "max_date": str(df[dataset.date_column].max().date()),
            "full_raw_paths": full_raw_paths,
            "full_normalized_paths": full_norm_paths,
            "annual_files": annual_files,
            "status": "ok",
        }

    def write_manifest(self, manifest: Dict[str, Any]) -> Path:
        path = self.metadata_dir / "run_manifest.json"

        with path.open("w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, default=str)

        self.logger.info(
            "Manifest written",
            extra={
                "event": "manifest_written",
                "run_id": manifest.get("run_id"),
                "path": str(path),
            },
        )

        return path


# =============================================================================
# MongoDB Storage
# =============================================================================

class TreasuryMongoStorage:
    def __init__(self, mongo_config: MongoConfig, logger: logging.Logger):
        self.mongo_config = mongo_config
        self.logger = logger

        self.client = MongoClient(mongo_config.uri)
        self.db: Database = self.client[mongo_config.database]

        self.datasets: Collection = self.db["datasets"]
        self.observations: Collection = self.db["observations"]
        self.ingestion_runs: Collection = self.db["ingestion_runs"]

    def close(self) -> None:
        self.client.close()

    def ping(self) -> None:
        self.client.admin.command("ping")

        self.logger.info(
            "MongoDB ping successful",
            extra={
                "event": "mongo_ping_successful",
                "database": self.mongo_config.database,
            },
        )

    def ensure_indexes(self) -> None:
        self.datasets.create_index(
            [("dataset_id", ASCENDING)],
            unique=True,
            name="uniq_dataset_id",
        )

        self.observations.create_index(
            [("dataset_id", ASCENDING), ("record_date", ASCENDING)],
            unique=True,
            name="uniq_dataset_record_date",
        )

        self.observations.create_index(
            [("record_date", ASCENDING)],
            name="idx_record_date",
        )

        self.observations.create_index(
            [("dataset_id", ASCENDING), ("year", ASCENDING)],
            name="idx_dataset_year",
        )

        self.ingestion_runs.create_index(
            [("run_id", ASCENDING)],
            unique=True,
            name="uniq_run_id",
        )

        self.logger.info(
            "MongoDB indexes ensured",
            extra={"event": "mongo_indexes_ensured"},
        )

    def start_run(self, run_id: str, datasets: List[DatasetConfig]) -> None:
        now = datetime.now(timezone.utc)

        doc = {
            "run_id": run_id,
            "status": "running",
            "started_at_utc": now,
            "finished_at_utc": None,
            "datasets_requested": [asdict(d) for d in datasets],
            "datasets_completed": [],
            "datasets_failed": [],
            "error_count": 0,
            "created_at_utc": now,
            "updated_at_utc": now,
        }

        self.ingestion_runs.update_one(
            {"run_id": run_id},
            {"$set": doc},
            upsert=True,
        )

        self.logger.info(
            "MongoDB ingestion run started",
            extra={
                "event": "mongo_ingestion_run_started",
                "run_id": run_id,
            },
        )

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        error_count: int,
        manifest_path: Optional[str],
    ) -> None:
        self.ingestion_runs.update_one(
            {"run_id": run_id},
            {
                "$set": {
                    "status": status,
                    "finished_at_utc": datetime.now(timezone.utc),
                    "updated_at_utc": datetime.now(timezone.utc),
                    "error_count": error_count,
                    "manifest_path": manifest_path,
                }
            },
            upsert=True,
        )

        self.logger.info(
            "MongoDB ingestion run finished",
            extra={
                "event": "mongo_ingestion_run_finished",
                "run_id": run_id,
                "status": status,
                "error_count": error_count,
                "manifest_path": manifest_path,
            },
        )

    def mark_dataset_completed(
        self,
        run_id: str,
        dataset_id: str,
        result: Dict[str, Any],
    ) -> None:
        self.ingestion_runs.update_one(
            {"run_id": run_id},
            {
                "$push": {
                    "datasets_completed": {
                        "dataset_id": dataset_id,
                        "completed_at_utc": datetime.now(timezone.utc),
                        "result": result,
                    }
                },
                "$set": {
                    "updated_at_utc": datetime.now(timezone.utc),
                },
            },
        )

    def mark_dataset_failed(
        self,
        run_id: str,
        dataset: DatasetConfig,
        error: Exception,
    ) -> None:
        self.ingestion_runs.update_one(
            {"run_id": run_id},
            {
                "$push": {
                    "datasets_failed": {
                        "dataset_id": dataset.folder_name,
                        "dataset_name": dataset.name,
                        "failed_at_utc": datetime.now(timezone.utc),
                        "error_type": type(error).__name__,
                        "error": repr(error),
                    }
                },
                "$inc": {"error_count": 1},
                "$set": {
                    "updated_at_utc": datetime.now(timezone.utc),
                },
            },
        )

    def upsert_dataset_metadata(
        self,
        dataset: DatasetConfig,
        df: pd.DataFrame,
        *,
        run_id: str,
    ) -> None:
        dataset_id = dataset.folder_name
        now = datetime.now(timezone.utc)

        if df.empty:
            min_date = None
            max_date = None
            total_rows = 0
            columns: List[str] = []
        else:
            min_date = df[dataset.date_column].min().to_pydatetime()
            max_date = df[dataset.date_column].max().to_pydatetime()
            total_rows = int(len(df))
            columns = list(df.columns)

        doc = {
            "dataset_id": dataset_id,
            "name": dataset.name,
            "data_key": dataset.data_key,
            "source_url": TreasuryXmlClient.BASE_URL,
            "folder_name": dataset.folder_name,
            "date_column": dataset.date_column,
            "start_year": dataset.start_year,
            "provider": "US Treasury Interest Rate XML Feed",
            "columns": columns,
            "min_record_date": min_date,
            "max_record_date": max_date,
            "last_ingested_run_id": run_id,
            "last_ingested_at_utc": now,
            "total_rows_last_seen": total_rows,
            "updated_at_utc": now,
        }

        self.datasets.update_one(
            {"dataset_id": dataset_id},
            {
                "$set": doc,
                "$setOnInsert": {"created_at_utc": now},
            },
            upsert=True,
        )

        self.logger.info(
            "Dataset metadata upserted",
            extra={
                "event": "mongo_dataset_metadata_upserted",
                "run_id": run_id,
                "dataset_id": dataset_id,
                "rows": total_rows,
                "min_date": min_date,
                "max_date": max_date,
            },
        )

    @staticmethod
    def _clean_value(value: Any) -> Any:
        if value is None:
            return None

        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass

        if isinstance(value, pd.Timestamp):
            dt = value.to_pydatetime()
            return dt.replace(tzinfo=timezone.utc)

        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if hasattr(value, "item"):
            return value.item()

        return value

    @staticmethod
    def _is_numeric_rate_field(column: str, value: Any) -> bool:
        if value is None:
            return False

        if not isinstance(value, (int, float)):
            return False

        col = column.lower()

        excluded_exact = {
            "year",
            "month",
            "day",
            "src_line_nbr",
        }

        if col in excluded_exact:
            return False

        excluded_contains = {
            "date",
            "cusip",
            "security",
            "desc",
            "unavail",
            "reason",
        }

        if any(token in col for token in excluded_contains):
            return False

        rate_tokens = {
            "bc",
            "tc",
            "round",
            "yield",
            "rate",
            "close",
            "avg",
            "coupon",
            "interest",
            "spread",
            "factor",
            "over",
            "years",
            "year",
            "month",
            "week",
            "wk",
        }

        return any(token in col for token in rate_tokens)

    def _row_to_observation_doc(
        self,
        row: pd.Series,
        dataset: DatasetConfig,
        *,
        run_id: str,
    ) -> Dict[str, Any]:
        dataset_id = dataset.folder_name
        dt: pd.Timestamp = row[dataset.date_column]

        if pd.isna(dt):
            raise ValueError(f"Missing record date for dataset={dataset_id}")

        record_dt = dt.to_pydatetime().replace(tzinfo=timezone.utc)

        raw = {
            col: self._clean_value(row[col])
            for col in row.index
        }

        rates = {
            col: raw[col]
            for col in row.index
            if col != dataset.date_column
            and self._is_numeric_rate_field(col, raw[col])
        }

        now = datetime.now(timezone.utc)

        return {
            "dataset_id": dataset_id,
            "record_date": record_dt,
            "year": record_dt.year,
            "month": record_dt.month,
            "day": record_dt.day,
            "source": {
                "provider": "US Treasury Interest Rate XML Feed",
                "data_key": dataset.data_key,
                "source_url": TreasuryXmlClient.BASE_URL,
            },
            "rates": rates,
            "raw": raw,
            "last_ingested_run_id": run_id,
            "updated_at_utc": now,
        }

    def upsert_observations(
        self,
        dataset: DatasetConfig,
        df: pd.DataFrame,
        *,
        run_id: str,
        batch_size: int,
    ) -> Dict[str, Any]:
        dataset_id = dataset.folder_name

        if df.empty:
            return {
                "dataset_id": dataset_id,
                "matched": 0,
                "modified": 0,
                "upserted": 0,
                "batches": 0,
            }

        total_matched = 0
        total_modified = 0
        total_upserted = 0
        batches = 0
        operations: List[UpdateOne] = []

        for _, row in df.iterrows():
            doc = self._row_to_observation_doc(row, dataset, run_id=run_id)

            operations.append(
                UpdateOne(
                    {
                        "dataset_id": doc["dataset_id"],
                        "record_date": doc["record_date"],
                    },
                    {
                        "$set": doc,
                        "$setOnInsert": {
                            "created_at_utc": datetime.now(timezone.utc),
                        },
                    },
                    upsert=True,
                )
            )

            if len(operations) >= batch_size:
                matched, modified, upserted = self._flush_observation_batch(
                    operations,
                    run_id=run_id,
                    dataset_id=dataset_id,
                )

                total_matched += matched
                total_modified += modified
                total_upserted += upserted
                batches += 1
                operations = []

        if operations:
            matched, modified, upserted = self._flush_observation_batch(
                operations,
                run_id=run_id,
                dataset_id=dataset_id,
            )

            total_matched += matched
            total_modified += modified
            total_upserted += upserted
            batches += 1

        result = {
            "dataset_id": dataset_id,
            "matched": total_matched,
            "modified": total_modified,
            "upserted": total_upserted,
            "batches": batches,
        }

        self.logger.info(
            "MongoDB observations upsert completed",
            extra={
                "event": "mongo_observations_upsert_completed",
                "run_id": run_id,
                **result,
            },
        )

        return result

    def _flush_observation_batch(
        self,
        operations: List[UpdateOne],
        *,
        run_id: str,
        dataset_id: str,
    ) -> Tuple[int, int, int]:
        try:
            result = self.observations.bulk_write(
                operations,
                ordered=False,
            )

            matched = int(result.matched_count)
            modified = int(result.modified_count)
            upserted = int(result.upserted_count)

            self.logger.info(
                "MongoDB observation batch upserted",
                extra={
                    "event": "mongo_observation_batch_upserted",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "batch_size": len(operations),
                    "matched": matched,
                    "modified": modified,
                    "upserted": upserted,
                },
            )

            return matched, modified, upserted

        except BulkWriteError as exc:
            self.logger.error(
                "MongoDB bulk write failed",
                extra={
                    "event": "mongo_bulk_write_failed",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "batch_size": len(operations),
                    "error": exc.details,
                },
                exc_info=True,
            )
            raise

        except PyMongoError as exc:
            self.logger.error(
                "MongoDB write failed",
                extra={
                    "event": "mongo_write_failed",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "batch_size": len(operations),
                    "error_type": type(exc).__name__,
                    "error": repr(exc),
                },
                exc_info=True,
            )
            raise


# =============================================================================
# Scraper
# =============================================================================

class BaseTreasuryScraper(ABC):
    def __init__(
        self,
        dataset: DatasetConfig,
        xml_client: TreasuryXmlClient,
        file_storage: TreasuryFileStorage,
        mongo_storage: TreasuryMongoStorage,
        config: ScraperConfig,
        logger: logging.Logger,
        run_id: str,
    ):
        self.dataset = dataset
        self.xml_client = xml_client
        self.file_storage = file_storage
        self.mongo_storage = mongo_storage
        self.config = config
        self.logger = logger
        self.run_id = run_id

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        pass

    @staticmethod
    def _convert_numeric_columns(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
        df = df.copy()

        for col in df.columns:
            if col == date_column:
                continue

            if df[col].dtype != "object":
                continue

            non_null_count = int(df[col].notna().sum())

            if non_null_count == 0:
                continue

            converted = pd.to_numeric(df[col], errors="coerce")
            converted_non_null_count = int(converted.notna().sum())

            if converted_non_null_count == non_null_count:
                df[col] = converted

        return df

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        dataset_id = self.dataset.folder_name

        self.logger.info(
            "Normalization started",
            extra={
                "event": "normalization_started",
                "run_id": self.run_id,
                "dataset_id": dataset_id,
                "input_rows": len(df),
                "input_columns": list(df.columns),
            },
        )

        if df.empty:
            return df

        df = df.copy()
        date_column = self.dataset.date_column

        if date_column not in df.columns:
            source_date_column = next(
                (
                    alias
                    for alias in self.dataset.date_column_aliases
                    if alias in df.columns
                ),
                None,
            )

            if source_date_column is not None:
                df = df.rename(columns={source_date_column: date_column})

        if self.dataset.date_column not in df.columns:
            raise ValueError(
                f"Missing date column {self.dataset.date_column} "
                f"for dataset {self.dataset.name}. Columns={list(df.columns)}"
            )

        df[self.dataset.date_column] = pd.to_datetime(
            df[self.dataset.date_column],
            errors="coerce",
        )

        before_date_drop = len(df)
        df = df.dropna(subset=[self.dataset.date_column])
        dropped_bad_dates = before_date_drop - len(df)

        df = self._convert_numeric_columns(df, self.dataset.date_column)

        for col in self.dataset.numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        before_dupes = len(df)
        df = df.drop_duplicates()
        dropped_duplicates = before_dupes - len(df)

        df = df.sort_values(self.dataset.date_column).reset_index(drop=True)

        self.logger.info(
            "Normalization completed",
            extra={
                "event": "normalization_completed",
                "run_id": self.run_id,
                "dataset_id": dataset_id,
                "output_rows": len(df),
                "dropped_bad_dates": dropped_bad_dates,
                "dropped_duplicates": dropped_duplicates,
                "min_date": df[self.dataset.date_column].min() if not df.empty else None,
                "max_date": df[self.dataset.date_column].max() if not df.empty else None,
            },
        )

        return df

    def run(self) -> Dict[str, Any]:
        dataset_id = self.dataset.folder_name

        self.logger.info(
            "Dataset scrape started",
            extra={
                "event": "dataset_scrape_started",
                "run_id": self.run_id,
                "dataset_id": dataset_id,
                "dataset_name": self.dataset.name,
                "data_key": self.dataset.data_key,
                "start_year": self.dataset.start_year,
            },
        )

        raw_df = self.fetch()
        normalized_df = self.normalize(raw_df)

        file_result = self.file_storage.persist_dataset(
            normalized_df,
            self.dataset,
            self.config,
            run_id=self.run_id,
        )

        self.mongo_storage.upsert_dataset_metadata(
            self.dataset,
            normalized_df,
            run_id=self.run_id,
        )

        mongo_result = self.mongo_storage.upsert_observations(
            self.dataset,
            normalized_df,
            run_id=self.run_id,
            batch_size=self.config.mongo_batch_size,
        )

        result = {
            "dataset_id": dataset_id,
            "dataset_name": self.dataset.name,
            "rows": int(len(normalized_df)),
            "file_result": file_result,
            "mongo_result": mongo_result,
            "status": "ok",
        }

        self.logger.info(
            "Dataset scrape completed",
            extra={
                "event": "dataset_scrape_completed",
                "run_id": self.run_id,
                "dataset_id": dataset_id,
                "rows": len(normalized_df),
                "mongo_result": mongo_result,
            },
        )

        return result


class TreasuryXmlYieldScraper(BaseTreasuryScraper):
    def fetch(self) -> pd.DataFrame:
        df = self.xml_client.fetch_dataset_history(
            self.dataset,
            run_id=self.run_id,
        )

        self.logger.info(
            "Treasury XML fetch completed",
            extra={
                "event": "treasury_xml_fetch_completed",
                "run_id": self.run_id,
                "dataset_id": self.dataset.folder_name,
                "data_key": self.dataset.data_key,
                "rows": len(df),
            },
        )

        return df


# =============================================================================
# Pipeline
# =============================================================================

class TreasuryYieldScraperPipeline:
    DEFAULT_DATASETS: List[DatasetConfig] = [
        DatasetConfig(
            name="Daily Treasury Par Yield Curve Rates",
            data_key="daily_treasury_yield_curve",
            folder_name="daily_treasury_par_yield_curve_rates",
            start_year=1990,
        ),
        DatasetConfig(
            name="Daily Treasury Bill Rates",
            data_key="daily_treasury_bill_rates",
            folder_name="daily_treasury_bill_rates",
            start_year=2002,
            date_column_aliases=("index_date", "quote_date"),
        ),
        DatasetConfig(
            name="Daily Treasury Long-Term Rates",
            data_key="daily_treasury_long_term_rate",
            folder_name="daily_treasury_long_term_rates",
            start_year=2000,
            date_column_aliases=("quote_date",),
            numeric_columns=("extrapolation_factor", "rate"),
        ),
        DatasetConfig(
            name="Daily Treasury Par Real Yield Curve Rates",
            data_key="daily_treasury_real_yield_curve",
            folder_name="daily_treasury_real_yield_curve_rates",
            start_year=2003,
        ),
        DatasetConfig(
            name="Daily Treasury Real Long-Term Rates",
            data_key="daily_treasury_real_long_term",
            folder_name="daily_treasury_real_long_term_rates",
            start_year=2000,
            date_column_aliases=("quote_date",),
            numeric_columns=("rate",),
        ),
    ]

    def __init__(
        self,
        config: ScraperConfig,
        mongo_config: MongoConfig,
        datasets: Optional[List[DatasetConfig]] = None,
    ):
        self.config = config
        self.mongo_config = mongo_config
        self.datasets = datasets or self.DEFAULT_DATASETS
        self.run_id = uuid.uuid4().hex

        self.logger = build_logger(config)
        self.xml_client = TreasuryXmlClient(config, self.logger)
        self.file_storage = TreasuryFileStorage(config.base_dir, self.logger)
        self.mongo_storage = TreasuryMongoStorage(mongo_config, self.logger)

    def run(self) -> Dict[str, Any]:
        manifest: Dict[str, Any] = {
            "run_id": self.run_id,
            "run_started_at_utc": datetime.now(timezone.utc).isoformat(),
            "base_dir": str(self.config.base_dir),
            "logs_dir": str(self.config.logs_dir),
            "mongo_database": self.mongo_config.database,
            "datasets": [],
            "errors": [],
        }

        try:
            self.logger.info(
                "Pipeline started",
                extra={
                    "event": "pipeline_started",
                    "run_id": self.run_id,
                    "datasets": [d.folder_name for d in self.datasets],
                },
            )

            self.mongo_storage.ping()
            self.mongo_storage.ensure_indexes()
            self.mongo_storage.start_run(self.run_id, self.datasets)

            for dataset in self.datasets:
                try:
                    scraper = TreasuryXmlYieldScraper(
                        dataset=dataset,
                        xml_client=self.xml_client,
                        file_storage=self.file_storage,
                        mongo_storage=self.mongo_storage,
                        config=self.config,
                        logger=self.logger,
                        run_id=self.run_id,
                    )

                    result = scraper.run()
                    manifest["datasets"].append(result)

                    self.mongo_storage.mark_dataset_completed(
                        self.run_id,
                        dataset.folder_name,
                        result,
                    )

                except Exception as exc:
                    error_doc = {
                        "dataset_id": dataset.folder_name,
                        "dataset_name": dataset.name,
                        "error_type": type(exc).__name__,
                        "error": repr(exc),
                    }

                    manifest["errors"].append(error_doc)

                    self.mongo_storage.mark_dataset_failed(
                        self.run_id,
                        dataset,
                        exc,
                    )

                    self.logger.error(
                        "Dataset failed",
                        extra={
                            "event": "dataset_failed",
                            "run_id": self.run_id,
                            **error_doc,
                        },
                        exc_info=True,
                    )

            status = "ok" if not manifest["errors"] else "partial_failure"

            manifest["run_finished_at_utc"] = datetime.now(timezone.utc).isoformat()
            manifest["status"] = status

            manifest_path = self.file_storage.write_manifest(manifest)
            manifest["manifest_path"] = str(manifest_path)

            self.mongo_storage.finish_run(
                self.run_id,
                status=status,
                error_count=len(manifest["errors"]),
                manifest_path=str(manifest_path),
            )

            self.logger.info(
                "Pipeline finished",
                extra={
                    "event": "pipeline_finished",
                    "run_id": self.run_id,
                    "status": status,
                    "datasets_completed": len(manifest["datasets"]),
                    "errors": len(manifest["errors"]),
                    "manifest_path": str(manifest_path),
                },
            )

            return manifest

        finally:
            self.mongo_storage.close()


# =============================================================================
# Read-Side Repository
# =============================================================================

class TreasuryYieldRepository:
    def __init__(self, mongo_config: MongoConfig):
        self.client = MongoClient(mongo_config.uri)
        self.db = self.client[mongo_config.database]
        self.observations = self.db["observations"]
        self.datasets = self.db["datasets"]
        self.ingestion_runs = self.db["ingestion_runs"]

    def close(self) -> None:
        self.client.close()

    def get_dataset_metadata(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        return self.datasets.find_one(
            {"dataset_id": dataset_id},
            {"_id": 0},
        )

    def get_history(
        self,
        dataset_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        query: Dict[str, Any] = {"dataset_id": dataset_id}

        if start_date or end_date:
            query["record_date"] = {}

            if start_date:
                query["record_date"]["$gte"] = start_date

            if end_date:
                query["record_date"]["$lte"] = end_date

        cursor = self.observations.find(
            query,
            {"_id": 0},
        ).sort("record_date", ASCENDING)

        rows = list(cursor)

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def get_rate_matrix(
        self,
        dataset_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        df = self.get_history(dataset_id, start_date, end_date)

        if df.empty:
            return df

        rates_df = pd.json_normalize(df["rates"])
        rates_df.insert(0, "record_date", df["record_date"])

        return rates_df.sort_values("record_date").reset_index(drop=True)

    def get_latest_run(self) -> Optional[Dict[str, Any]]:
        return self.ingestion_runs.find_one(
            {},
            {"_id": 0},
            sort=[("started_at_utc", -1)],
        )


# =============================================================================
# Entrypoint
# =============================================================================

def main() -> None:
    load_dotenv()

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DB", "us_treasury_market_data")

    if not mongo_uri:
        raise RuntimeError("Missing MONGODB_URI environment variable")

    config = ScraperConfig(
        base_dir=Path("us_treasury_yields"),
        logs_dir=Path("logs"),
        request_timeout=60,
        max_retries=5,
        retry_sleep_seconds=1.5,
        write_csv=True,
        write_parquet=True,
        mongo_batch_size=2_000,
        log_lines_per_file=5_000,
    )

    mongo_config = MongoConfig(
        uri=mongo_uri,
        database=mongo_db,
    )

    pipeline = TreasuryYieldScraperPipeline(
        config=config,
        mongo_config=mongo_config,
    )

    manifest = pipeline.run()
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
