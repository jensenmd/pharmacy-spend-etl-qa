"""
test_etl_validation.py

SQL-driven data validation test suite for the pharmacy spend ETL pipeline.
Validates source-to-target data integrity, business rules, and data quality.

Mirrors the QA validation approach used at GHX on the Rx Analytics platform:
  - Risk-based spot-checking informed by business rules
  - Multi-layer validation (source → warehouse → aggregates)
  - SQL queries against the data warehouse to verify transformation accuracy

Run: pytest tests/ -v
"""

import pytest
import sqlite3
import csv
import os

DB_PATH = "data/processed/pharmacy_warehouse.db"
SOURCE_FILE = "data/raw/part_d_spending_2022.csv"


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def db():
    """Open warehouse DB connection for the test session."""
    assert os.path.exists(DB_PATH), f"Warehouse DB not found: {DB_PATH}. Run etl_pipeline.py first."
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def source_records():
    """Load source CSV records for source-to-target comparison."""
    assert os.path.exists(SOURCE_FILE), f"Source file not found: {SOURCE_FILE}"
    with open(SOURCE_FILE, newline="") as f:
        return list(csv.DictReader(f))


# ─────────────────────────────────────────────
# 1. Row Count Reconciliation
# ─────────────────────────────────────────────

class TestRowCounts:

    def test_fact_table_row_count_matches_source(self, db, source_records):
        """
        Warehouse fact table row count must match source file record count.
        Catches truncation, duplication, or missed records during load.
        """
        source_count = len(source_records)
        cursor = db.execute("SELECT COUNT(*) FROM fact_drug_spend")
        warehouse_count = cursor.fetchone()[0]
        assert warehouse_count == source_count, (
            f"Row count mismatch: source={source_count}, warehouse={warehouse_count}"
        )

    def test_dim_drug_has_expected_unique_drugs(self, db, source_records):
        """
        Dimension table should contain exactly the unique drug/generic combinations
        from the source file.
        """
        source_drugs = {(r["drug_name"].strip().upper(), r["generic_name"].strip().upper())
                       for r in source_records}
        cursor = db.execute("SELECT drug_name, generic_name FROM dim_drug")
        warehouse_drugs = {(row["drug_name"], row["generic_name"]) for row in cursor.fetchall()}
        assert warehouse_drugs == source_drugs, (
            f"Drug dimension mismatch.\n"
            f"In source but not warehouse: {source_drugs - warehouse_drugs}\n"
            f"In warehouse but not source: {warehouse_drugs - source_drugs}"
        )

    def test_dim_manufacturer_has_expected_manufacturers(self, db, source_records):
        """
        Manufacturer dimension should contain all unique manufacturers from source.
        """
        source_mfrs = {r["manufacturer"].strip() for r in source_records}
        cursor = db.execute("SELECT manufacturer_name FROM dim_manufacturer")
        warehouse_mfrs = {row["manufacturer_name"] for row in cursor.fetchall()}
        assert warehouse_mfrs == source_mfrs


# ─────────────────────────────────────────────
# 2. Spend Total Validation
# ─────────────────────────────────────────────

class TestSpendTotals:

    def test_total_spending_matches_source(self, db, source_records):
        """
        Sum of total_spending in warehouse must match sum from source file.
        Core financial accuracy check — if this fails, dollars are wrong.
        """
        source_total = sum(float(r["total_spending"]) for r in source_records)
        cursor = db.execute("SELECT ROUND(SUM(total_spending), 2) FROM fact_drug_spend")
        warehouse_total = cursor.fetchone()[0]
        assert abs(warehouse_total - round(source_total, 2)) < 0.01, (
            f"Total spending mismatch: source={round(source_total, 2)}, "
            f"warehouse={warehouse_total}"
        )

    def test_total_claims_matches_source(self, db, source_records):
        """
        Sum of claim_count in warehouse must match source.
        """
        source_claims = sum(int(r["claim_count"]) for r in source_records)
        cursor = db.execute("SELECT SUM(claim_count) FROM fact_drug_spend")
        warehouse_claims = cursor.fetchone()[0]
        assert warehouse_claims == source_claims, (
            f"Claim count mismatch: source={source_claims}, warehouse={warehouse_claims}"
        )

    def test_spending_per_claim_calculation_accuracy(self, db):
        """
        Validate spending_per_claim against total_spending / claim_count.
        Catches transformation errors in derived field calculations.
        Spot-checks records where the ratio is off by more than 1%.
        """
        cursor = db.execute("""
            SELECT spend_id, total_spending, claim_count, spending_per_claim
            FROM fact_drug_spend
            WHERE claim_count > 0
        """)
        rows = cursor.fetchall()
        mismatches = []
        for row in rows:
            expected = round(row["total_spending"] / row["claim_count"], 2)
            actual = row["spending_per_claim"]
            pct_diff = abs(expected - actual) / expected if expected != 0 else 0
            if pct_diff > 0.01:  # Allow 1% tolerance for rounding
                mismatches.append({
                    "spend_id": row["spend_id"],
                    "expected": expected,
                    "actual": actual,
                    "pct_diff": round(pct_diff * 100, 2)
                })
        assert len(mismatches) == 0, (
            f"{len(mismatches)} records have spending_per_claim > 1% off from "
            f"total_spending/claim_count: {mismatches[:5]}"
        )


# ─────────────────────────────────────────────
# 3. Manufacturer Rollup Validation
# ─────────────────────────────────────────────

class TestManufacturerRollups:

    def test_aggregate_manufacturer_spend_matches_fact(self, db):
        """
        Pre-computed manufacturer spend aggregates must match
        direct aggregation from the fact table.
        Validates the rollup calculation accuracy.
        """
        cursor = db.execute("""
            SELECT
                a.manufacturer_id,
                a.total_spending     AS agg_spending,
                f.fact_spending
            FROM agg_manufacturer_spend a
            JOIN (
                SELECT manufacturer_id,
                       ROUND(SUM(total_spending), 2) AS fact_spending
                FROM fact_drug_spend
                GROUP BY manufacturer_id
            ) f ON a.manufacturer_id = f.manufacturer_id
        """)
        rows = cursor.fetchall()
        mismatches = [r for r in rows if abs(r["agg_spending"] - r["fact_spending"]) > 0.01]
        assert len(mismatches) == 0, (
            f"{len(mismatches)} manufacturers have aggregate spend mismatches: {mismatches}"
        )

    def test_aggregate_claim_counts_match_fact(self, db):
        """
        Aggregate claim counts per manufacturer must match fact table sums.
        """
        cursor = db.execute("""
            SELECT
                a.manufacturer_id,
                a.total_claims       AS agg_claims,
                SUM(f.claim_count)   AS fact_claims
            FROM agg_manufacturer_spend a
            JOIN fact_drug_spend f ON a.manufacturer_id = f.manufacturer_id
            GROUP BY a.manufacturer_id, a.total_claims
        """)
        rows = cursor.fetchall()
        mismatches = [r for r in rows if r["agg_claims"] != r["fact_claims"]]
        assert len(mismatches) == 0

    def test_every_manufacturer_has_aggregate_record(self, db):
        """
        Every manufacturer in the fact table must have a corresponding
        aggregate record. Catches gaps in the rollup process.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT manufacturer_id FROM fact_drug_spend
                EXCEPT
                SELECT manufacturer_id FROM agg_manufacturer_spend
            )
        """)
        missing = cursor.fetchone()[0]
        assert missing == 0, f"{missing} manufacturers in fact table have no aggregate record"


# ─────────────────────────────────────────────
# 4. Drug Category Validation
# ─────────────────────────────────────────────

class TestDrugCategories:

    def test_all_drugs_have_valid_category(self, db):
        """
        All drugs must be assigned to a known category.
        Catches miscategorization or null category values from ETL.
        """
        valid_categories = {
            "Cardiovascular", "Diabetes", "Oncology", "Immunology",
            "Neurology", "Respiratory", "Infectious Disease", "Gastroenterology"
        }
        cursor = db.execute("SELECT DISTINCT drug_category FROM dim_drug")
        warehouse_categories = {row["drug_category"] for row in cursor.fetchall()}
        invalid = warehouse_categories - valid_categories
        assert len(invalid) == 0, f"Invalid drug categories found: {invalid}"

    def test_category_spend_rollup_matches_total(self, db):
        """
        Sum of spending by category must equal total warehouse spending.
        Validates that category assignments cover all records with no gaps.
        """
        cursor = db.execute("SELECT ROUND(SUM(total_spending), 2) FROM fact_drug_spend")
        total = cursor.fetchone()[0]

        cursor = db.execute("""
            SELECT ROUND(SUM(f.total_spending), 2)
            FROM fact_drug_spend f
            JOIN dim_drug d ON f.drug_id = d.drug_id
        """)
        category_total = cursor.fetchone()[0]
        assert abs(total - category_total) < 0.01


# ─────────────────────────────────────────────
# 5. Data Quality Checks
# ─────────────────────────────────────────────

class TestDataQuality:

    def test_no_null_required_fields_in_fact_table(self, db):
        """
        Required fields in the fact table must have no NULL values.
        Catches incomplete records that slipped through transformation.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM fact_drug_spend
            WHERE drug_id IS NULL
               OR manufacturer_id IS NULL
               OR year IS NULL
               OR claim_count IS NULL
               OR total_spending IS NULL
               OR spending_per_claim IS NULL
               OR beneficiary_count IS NULL
               OR unit_count IS NULL
        """)
        null_count = cursor.fetchone()[0]
        assert null_count == 0, f"{null_count} fact table records have NULL required fields"

    def test_no_negative_financial_values(self, db):
        """
        Spending values must not be negative.
        Catches sign errors or bad source data that passed through ETL.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM fact_drug_spend
            WHERE total_spending < 0
               OR spending_per_claim < 0
        """)
        negative_count = cursor.fetchone()[0]
        assert negative_count == 0, f"{negative_count} records have negative spending values"

    def test_no_zero_claim_counts(self, db):
        """
        Claim counts must be greater than zero.
        A record with zero claims should not exist in the warehouse.
        """
        cursor = db.execute("SELECT COUNT(*) FROM fact_drug_spend WHERE claim_count <= 0")
        zero_count = cursor.fetchone()[0]
        assert zero_count == 0, f"{zero_count} records have zero or negative claim counts"

    def test_beneficiary_count_not_greater_than_claim_count(self, db):
        """
        Beneficiary count should not exceed claim count.
        One beneficiary can have multiple claims, but not vice versa.
        Business rule catch.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM fact_drug_spend
            WHERE beneficiary_count > claim_count
        """)
        invalid_count = cursor.fetchone()[0]
        assert invalid_count == 0, (
            f"{invalid_count} records have beneficiary_count > claim_count"
        )

    def test_drug_names_are_uppercase(self, db):
        """
        ETL transform normalizes drug names to uppercase.
        Verify transformation was applied correctly.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM dim_drug
            WHERE drug_name != UPPER(drug_name)
               OR generic_name != UPPER(generic_name)
        """)
        non_upper = cursor.fetchone()[0]
        assert non_upper == 0, f"{non_upper} drug names not normalized to uppercase"

    def test_year_values_are_valid(self, db):
        """
        Year must be a plausible Medicare Part D year (2006 onward).
        Catches date transformation errors.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM fact_drug_spend
            WHERE year < 2006 OR year > 2030
        """)
        invalid_years = cursor.fetchone()[0]
        assert invalid_years == 0, f"{invalid_years} records have invalid year values"

    def test_no_duplicate_fact_records(self, db):
        """
        No duplicate records in the fact table for the same
        drug/manufacturer/year combination with identical spend values.
        Catches double-load bugs.
        """
        cursor = db.execute("""
            SELECT drug_id, manufacturer_id, year,
                   claim_count, total_spending, COUNT(*) as cnt
            FROM fact_drug_spend
            GROUP BY drug_id, manufacturer_id, year, claim_count, total_spending
            HAVING cnt > 1
        """)
        duplicates = cursor.fetchall()
        assert len(duplicates) == 0, f"{len(duplicates)} duplicate fact records found"


# ─────────────────────────────────────────────
# 6. Foreign Key Integrity
# ─────────────────────────────────────────────

class TestReferentialIntegrity:

    def test_all_fact_drug_ids_exist_in_dim_drug(self, db):
        """
        Every drug_id in the fact table must reference a valid dim_drug record.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM fact_drug_spend f
            LEFT JOIN dim_drug d ON f.drug_id = d.drug_id
            WHERE d.drug_id IS NULL
        """)
        orphans = cursor.fetchone()[0]
        assert orphans == 0, f"{orphans} fact records reference non-existent drug_id"

    def test_all_fact_manufacturer_ids_exist_in_dim_manufacturer(self, db):
        """
        Every manufacturer_id in the fact table must reference a valid
        dim_manufacturer record.
        """
        cursor = db.execute("""
            SELECT COUNT(*) FROM fact_drug_spend f
            LEFT JOIN dim_manufacturer m ON f.manufacturer_id = m.manufacturer_id
            WHERE m.manufacturer_id IS NULL
        """)
        orphans = cursor.fetchone()[0]
        assert orphans == 0, f"{orphans} fact records reference non-existent manufacturer_id"
