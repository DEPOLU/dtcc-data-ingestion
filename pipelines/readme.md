# Azure Data Factory (ADF) Integration

## Overview
This project uses ADF to orchestrate the daily processing of DTCC MRO files...

## Pipeline Logic
- **GetMetadata**: List all `.mro` files in the source container
- **Filter**: Keep only files that end with `.mro`
- **ForEach**: Run Databricks notebook once for each file...
