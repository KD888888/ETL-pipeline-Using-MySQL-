## ETL-pipeline-Using-MySQL-
End-to-end SQL-based Data Pipeline automating ETL across Source, Staging, ODS, DWH, and Data Mart layers. Includes audit logging, data quality checks, and automated summary views—ideal for demonstrating full lifecycle data engineering and governance best practices.

**Architecture Overview******

Source Layer: Raw data tables (raw_customers, raw_products, etc.)

Staging Layer: Data ingestion, timestamping, and initial validation

ODS (Operational Data Store): Data cleansing, standardization, and typing

DWH (Data Warehouse): Fact and Dimension tables for analytics

DM (Data Marts): Finance, Marketing, and HR-specific datasets

Control Layer: Logging and Data Quality audits for governance

**Core ETL Procedures******

SourceToStaging() – Transfers raw data to staging with source tagging

StagingToODS() – Cleans and validates records for quality consistency

PopulateDataQualityAudit(audit_id) – Logs field-level data issues

BuildAuditSummary(audit_id) – Aggregates audit metrics

LoadODSToDWH() – Builds dimension and fact models

LoadDWHToDM() – Pushes curated data to business marts

**Tech Stack******

Database: MySQL

Language: SQL / Stored Procedures

Data Flow: Source → Staging → ODS → DWH → DM

Audit Framework: Control tables, DQ audit, and summary views

**Why It Matters?******

This project reflects best practices in data warehousing:

Clean separation between data ingestion, transformation, and presentation

Full audit trail for transparency and reproducibility

SQL-only implementation to demonstrate proficiency without external ETL tools
