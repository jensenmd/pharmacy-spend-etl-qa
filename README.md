# pharmacy-spend-etl-qa

A SQL-driven data validation framework simulating ETL pipeline QA using pharmaceutical spend data modeled after the CMS Medicare Part D public dataset.

Built to demonstrate the source-to-target validation discipline used in enterprise healthcare QA — specifically the approach developed over five years testing the **Rx Analytics platform at GHX (Global Healthcare Exchange)**.

![CI Status](https://github.com/jensenmd/pharmacy-spend-etl-qa/actions/workflows/ci.yml/badge.svg)

Built by **Michael D. Jensen** — Senior QA Engineer with 15+ years of enterprise testing experience, including five years as primary QA engineer on a pharmaceutical spend analytics platform in a HIPAA-compliant environment.

🔗 [LinkedIn](https://www.linkedin.com/in/michael-jensen-751b59294/) | 📧 jensen.md@gmail.com

---

## What It Does

Simulates a three-stage pharmacy spend data pipeline and validates each stage:

```
Source CSV files          →     ETL Pipeline          →     SQLite Warehouse
(raw pharmacy spend data)       (transform + load)          (fact + dim tables)
        ↑                                                           ↓
  generate_source_data.py           etl_pipeline.py         pytest validation suite
```

The pytest suite runs SQL queries directly against the warehouse and validates:
- Row count reconciliation (source vs. warehouse)
- Financial accuracy (spend totals, claim counts)
- Manufacturer rollup integrity (aggregate vs. fact table)
- Business rule validation (beneficiary logic, category assignments)
- Data quality checks (nulls, negatives, duplicates, formatting)
- Referential integrity (foreign key consistency)

This is the hardest QA work to fake — either the numbers reconcile end-to-end or they don't. The validation approach here mirrors what real ETL QA looks like in production: targeted SQL queries against the warehouse, reconciled against source data, with coverage driven by knowledge of where business rule calculations are most likely to break.

---

## Project Structure

```
pharmacy-spend-etl-qa/
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions: generate → ETL → validate
├── data/
│   ├── generate_source_data.py     # Synthetic Part D data generator
│   └── raw/                        # Generated source CSV files
│   └── processed/                  # SQLite warehouse database
├── etl/
│   └── etl_pipeline.py             # Extract → Transform → Load pipeline
├── tests/
│   └── test_etl_validation.py      # pytest validation suite (SQL-driven)
├── reports/                        # Generated HTML test reports + ETL logs
├── conftest.py
└── requirements.txt
```

---

## Test Coverage

| Category | Tests | What's Validated |
|---|---|---|
| Row Count Reconciliation | 3 | Source vs. warehouse counts, dimension completeness |
| Spend Total Validation | 3 | Total spending, claim counts, spending-per-claim accuracy |
| Manufacturer Rollups | 3 | Aggregate vs. fact table, claim count sums, coverage |
| Drug Category Validation | 2 | Valid categories, category spend rollup completeness |
| Data Quality | 6 | Nulls, negatives, zero claims, uppercase normalization, year validity, duplicates |
| Referential Integrity | 2 | Foreign key consistency across fact and dimension tables |
| **Total** | **19** | |

---

## Data Model

```
dim_manufacturer          dim_drug
─────────────────         ──────────────────────
manufacturer_id (PK)      drug_id (PK)
manufacturer_name         drug_name
                          generic_name
                          drug_category
         │                      │
         └──────────┬───────────┘
                    ↓
            fact_drug_spend
            ───────────────────────
            spend_id (PK)
            drug_id (FK)
            manufacturer_id (FK)
            year
            claim_count
            total_spending
            spending_per_claim
            beneficiary_count
            unit_count
            load_timestamp
                    │
                    ↓
        agg_manufacturer_spend
        ───────────────────────────
        Pre-computed rollup by manufacturer/year
```

---

## CI Pipeline

GitHub Actions runs on every push to `main`:
1. Generates synthetic source data
2. Runs the ETL pipeline
3. Executes the full validation suite
4. Uploads HTML report as a downloadable artifact

---

## Background

This project mirrors the QA validation workflow used on the **Rx Analytics platform at GHX (Global Healthcare Exchange)**, where the primary QA discipline was validating pharmaceutical spend data flowing through:

- Nightly batch ingestion from pharmacy source files
- Informatica ETL transformation
- SQL Server data warehouse
- Business Objects BI reporting layer

The core validation approach — risk-based SQL spot-checking against the warehouse, reconciling source data against transformed output, validating business rule calculations — is directly reflected in this test suite.

The business context was real and high-stakes: pharmacy chains and hospital pharmacy systems used these reports to make purchasing decisions worth millions of dollars annually. Validating that tiered pricing thresholds, brand vs. generic spend calculations, and savings opportunity reporting were accurate wasn't just a QA exercise — it was ensuring that financial recommendations to clients were correct. That context shaped how this validation suite was designed: coverage driven by where errors would have the most impact, not just by what was easiest to test.

---

## Running Locally

**Prerequisites:** Python 3.9+

```bash
# Install dependencies
pip install -r requirements.txt

# Step 1: Generate source data
python data/generate_source_data.py

# Step 2: Run ETL pipeline
python etl/etl_pipeline.py

# Step 3: Run validation suite
pytest tests/ -v

# Optional: generate HTML report
pytest tests/ -v --html=reports/validation-report.html --self-contained-html
```

---

## Relationship to Other Portfolio Projects

This project is part of a three-project QA portfolio demonstrating complementary skills:

| Project | Focus | Stack |
|---|---|---|
| **pharmacy-spend-etl-qa** (this repo) | ETL pipeline validation, SQL-driven data integrity testing | Python / pytest / SQLite / pandas |
| [qa-automation-showcase](https://github.com/jensenmd/qa-automation-showcase) | REST API testing, data validation, CI/CD integration | Python / pytest / Postman / GitHub Actions |
| [restful-booker-qa](https://github.com/jensenmd/restful-booker-qa) | Full-stack layered testing — API + UI automation | Postman / Newman / Playwright / GitHub Actions |

Together they demonstrate backend data validation, API testing, and UI automation — the core layers of a modern QA engineering practice.

---

## Author

**Michael D. Jensen** — Senior QA Engineer
15+ years of enterprise software testing experience across healthcare IT, financial systems, telecommunications, and cybersecurity. Deep background in REST API validation, ETL pipeline testing, SQL-based data integrity verification, and full-stack manual testing in Agile environments.

Five years as primary QA engineer on a pharmaceutical spend analytics platform — validating complex business rules, ETL pipelines, and financial reporting in a HIPAA-compliant environment.

🔗 [LinkedIn](https://www.linkedin.com/in/michael-jensen-751b59294/) | 🐙 [GitHub Profile](https://github.com/jensenmd) | 📧 jensen.md@gmail.com
