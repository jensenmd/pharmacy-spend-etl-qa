"""
etl_pipeline.py

Simulates the ETL pipeline for pharmacy spend data.
Reads raw source CSV → transforms → loads into SQLite data warehouse.

Mirrors the GHX Rx Analytics pipeline:
  Source files → Informatica ETL → SQL Server data warehouse

Run: python etl/etl_pipeline.py
"""

import csv
import sqlite3
import os
import sys
import logging
from datetime import datetime

# Setup logging — mirrors production pipeline log monitoring
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("reports/etl_pipeline.log")
    ]
)
log = logging.getLogger(__name__)

SOURCE_FILE = "data/raw/part_d_spending_2022.csv"
DB_PATH = "data/processed/pharmacy_warehouse.db"


def create_warehouse_schema(conn):
    """
    Create data warehouse tables.
    Mirrors a simplified version of a pharmacy spend data warehouse schema.
    """
    cursor = conn.cursor()

    # Dimension table: drugs
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_drug (
            drug_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            drug_name   TEXT NOT NULL,
            generic_name TEXT NOT NULL,
            drug_category TEXT NOT NULL,
            UNIQUE(drug_name, generic_name)
        )
    """)

    # Dimension table: manufacturers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_manufacturer (
            manufacturer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manufacturer_name TEXT NOT NULL UNIQUE
        )
    """)

    # Fact table: spend
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_drug_spend (
            spend_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            drug_id             INTEGER NOT NULL,
            manufacturer_id     INTEGER NOT NULL,
            year                INTEGER NOT NULL,
            claim_count         INTEGER NOT NULL,
            total_spending      REAL NOT NULL,
            spending_per_claim  REAL NOT NULL,
            beneficiary_count   INTEGER NOT NULL,
            unit_count          INTEGER NOT NULL,
            load_timestamp      TEXT NOT NULL,
            FOREIGN KEY (drug_id) REFERENCES dim_drug(drug_id),
            FOREIGN KEY (manufacturer_id) REFERENCES dim_manufacturer(manufacturer_id)
        )
    """)

    # Aggregate table: manufacturer spend summary (pre-computed rollup)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_manufacturer_spend (
            agg_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            manufacturer_id     INTEGER NOT NULL,
            year                INTEGER NOT NULL,
            total_claims        INTEGER NOT NULL,
            total_spending      REAL NOT NULL,
            avg_spending_per_claim REAL NOT NULL,
            drug_count          INTEGER NOT NULL,
            load_timestamp      TEXT NOT NULL,
            FOREIGN KEY (manufacturer_id) REFERENCES dim_manufacturer(manufacturer_id)
        )
    """)

    conn.commit()
    log.info("Warehouse schema created/verified")


def load_source_data(filepath):
    """Read raw CSV source file."""
    records = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    log.info(f"Loaded {len(records)} records from source: {filepath}")
    return records


def transform(records):
    """
    Apply transformation rules to source records.
    - Normalize text fields
    - Cast numeric fields
    - Validate required fields
    - Calculate derived fields
    """
    transformed = []
    skipped = 0

    for row in records:
        try:
            # Validate required fields
            required = ["drug_name", "generic_name", "manufacturer",
                       "drug_category", "claim_count", "total_spending",
                       "spending_per_claim", "beneficiary_count", "unit_count", "year"]
            if any(row.get(f, "").strip() == "" for f in required):
                log.warning(f"Skipping record with missing required fields: {row}")
                skipped += 1
                continue

            transformed.append({
                "drug_name":          row["drug_name"].strip().upper(),
                "generic_name":       row["generic_name"].strip().upper(),
                "manufacturer":       row["manufacturer"].strip(),
                "drug_category":      row["drug_category"].strip(),
                "claim_count":        int(row["claim_count"]),
                "total_spending":     round(float(row["total_spending"]), 2),
                "spending_per_claim": round(float(row["spending_per_claim"]), 2),
                "beneficiary_count":  int(row["beneficiary_count"]),
                "unit_count":         int(row["unit_count"]),
                "year":               int(row["year"]),
                "load_timestamp":     datetime.now().isoformat()
            })
        except (ValueError, KeyError) as e:
            log.error(f"Transform error on record {row}: {e}")
            skipped += 1

    log.info(f"Transform complete: {len(transformed)} records transformed, {skipped} skipped")
    return transformed


def load_to_warehouse(conn, records):
    """Load transformed records into warehouse tables."""
    cursor = conn.cursor()
    load_timestamp = datetime.now().isoformat()

    # Clear existing data for clean load
    cursor.execute("DELETE FROM fact_drug_spend")
    cursor.execute("DELETE FROM agg_manufacturer_spend")
    cursor.execute("DELETE FROM dim_drug")
    cursor.execute("DELETE FROM dim_manufacturer")
    log.info("Cleared existing warehouse data for clean load")

    # Load dimension tables first
    for record in records:
        cursor.execute("""
            INSERT OR IGNORE INTO dim_drug (drug_name, generic_name, drug_category)
            VALUES (?, ?, ?)
        """, (record["drug_name"], record["generic_name"], record["drug_category"]))

        cursor.execute("""
            INSERT OR IGNORE INTO dim_manufacturer (manufacturer_name)
            VALUES (?)
        """, (record["manufacturer"],))

    conn.commit()
    log.info("Dimension tables loaded")

    # Load fact table
    fact_rows = 0
    for record in records:
        cursor.execute("SELECT drug_id FROM dim_drug WHERE drug_name = ? AND generic_name = ?",
                      (record["drug_name"], record["generic_name"]))
        drug_id = cursor.fetchone()[0]

        cursor.execute("SELECT manufacturer_id FROM dim_manufacturer WHERE manufacturer_name = ?",
                      (record["manufacturer"],))
        manufacturer_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO fact_drug_spend
            (drug_id, manufacturer_id, year, claim_count, total_spending,
             spending_per_claim, beneficiary_count, unit_count, load_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (drug_id, manufacturer_id, record["year"], record["claim_count"],
              record["total_spending"], record["spending_per_claim"],
              record["beneficiary_count"], record["unit_count"], load_timestamp))
        fact_rows += 1

    conn.commit()
    log.info(f"Fact table loaded: {fact_rows} rows")

    # Build aggregate table — pre-computed manufacturer rollups
    cursor.execute("""
        INSERT INTO agg_manufacturer_spend
        (manufacturer_id, year, total_claims, total_spending,
         avg_spending_per_claim, drug_count, load_timestamp)
        SELECT
            f.manufacturer_id,
            f.year,
            SUM(f.claim_count)                              AS total_claims,
            ROUND(SUM(f.total_spending), 2)                 AS total_spending,
            ROUND(AVG(f.spending_per_claim), 2)             AS avg_spending_per_claim,
            COUNT(DISTINCT f.drug_id)                       AS drug_count,
            ?
        FROM fact_drug_spend f
        GROUP BY f.manufacturer_id, f.year
    """, (load_timestamp,))

    conn.commit()
    log.info("Aggregate table built")

    return fact_rows


def run_pipeline():
    """Main pipeline execution."""
    log.info("=" * 60)
    log.info("ETL PIPELINE STARTING")
    log.info("=" * 60)

    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("reports", exist_ok=True)

    # Extract
    source_records = load_source_data(SOURCE_FILE)
    source_count = len(source_records)

    # Transform
    transformed = transform(source_records)
    transformed_count = len(transformed)

    # Load
    conn = sqlite3.connect(DB_PATH)
    create_warehouse_schema(conn)
    loaded_count = load_to_warehouse(conn, transformed)
    conn.close()

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE")
    log.info(f"  Source records:      {source_count}")
    log.info(f"  Transformed:         {transformed_count}")
    log.info(f"  Loaded to warehouse: {loaded_count}")
    log.info(f"  Database:            {DB_PATH}")
    log.info("=" * 60)

    return source_count, transformed_count, loaded_count


if __name__ == "__main__":
    run_pipeline()
