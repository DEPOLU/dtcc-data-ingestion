# DTCC Ingestion Pipeline (Azure Databricks + ADF)

This repository contains a production-ready pipeline for ingesting DTCC MRO files, parsing 15+ record types using defined layout mappings, and outputting structured CSVs. The pipeline supports both automated and manual execution, with integration into Azure Data Factory for scheduled or event-driven processing.

---

## Features

- Automated ingestion via Azure Data Factory
- ADF integration with parameterized notebook execution
- Single file mode (for ADF) and batch mode (manual)
- Robust layout-based parsing of DTCC MRO files
- Handles enrichment and file archival
- Outputs structured CSVs to Azure Blob Storage


## Technologies Used

- Azure Databricks (PySpark)
- Azure Data Factory (ADF)
- Azure Data Lake Gen2
- Azure Blob Storage
- Python (Pandas, Regex)
- GitHub

---

## ADF Pipeline: `dtccdailyprocessing`

This ADF pipeline orchestrates the parsing of new `.mro` files:

### Steps:
1. **GetNewMROFiles** – Retrieves all `.mro` files using the `Binary1` dataset.
2. **Filter_mro_files** – Filters only `.mro` files using a conditional expression.
3. **ProcessEachMROFile** – Loops over each file in parallel and triggers a Databricks notebook job.



## Output

Processed files are written to the `parsed` container as CSVs, with filenames matching the record type (e.g., `contract_record.csv`, `contract_valuation.csv`). Files are archived to `processed/YYYY-MM-DD/` and a summary log is saved to `logs/YYYY-MM-DD/`.

---

## Record Types Supported

- Submitting Header
- Contra Record
- Contract Record
- Contract Valuation
- Contract Underlying Asset
- Contract Index Loop
- Contract Band Guaranteed Loop
- Contract Agent Record
- Contract Dates Record
- Contract Events Record
- Contract Party Record
- Contract Party Address Record
- Contract Annuitization Payout Record
- Contract Party Communication Record
- Contract Service Feature Record

---

## Setup & Deployment

1. Upload the Databricks notebook (`dtcc_pipeline_adf_integration.py`) to your workspace.
2. Configure blob containers: `source`, `parsed`, `processed`, and `logs`.
3. Import the `dtccdailyprocessing.json` ADF pipeline via Azure Data Factory Studio.

---

---

## 👤 Maintainer

Vikas Dabas – [LinkedIn](https://www.linkedin.com/in/vikasdabas)

