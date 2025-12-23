# DWL_HazardAirQualityImpactMonitor

This repository contains the full technical implementation of the semester project **“Hazard–Air Quality Impact Monitor”**, developed in the context of the *Data Warehouse and Data Lake Systems* course at the Lucerne university of applied sciences and arts (HSLU).

The project implements an AWS-based data lake and data warehouse pipeline to ingest, transform, validate, and analyze hazard-related data (earthquakes, fires) together with air-quality observations. The system supports descriptive analytics and a simple operational risk indicator for short-term air-quality exceedances.

## Repository Structure

DWL_HazardAirQualityImpactMonitor/  
├── Lambda/  
│   ├── API_Air_Quality/  
│   │   └── lambda_function.py  
│   ├── API_Earthquakes/  
│   │   └── lambda_function.py  
│   ├── API_Fires/  
│   │   └── lambda_function.py  
│   └── RQ3/  
│       └── lambda_function.py  
│  
├── AWS_Glue/  
│   ├── dw-clean_staging.json  
│   ├── dw-load_rds.json  
│   └── dw-data_quality.json  
│  
└── README.md  


## Architecture Overview

The pipeline follows a **Bronze–Silver–Gold (Medallion) architecture**:

- **Bronze**: Raw API data ingested and stored unchanged.
- **Silver**: Cleaned, standardized staging tables.
- **Gold**: Dimensional data warehouse optimized for analytics and risk scoring.

AWS services used:
- **AWS Lambda** for API-based ingestion and operational analytics (RQ3).
- **AWS Glue** for orchestration of transformation and validation jobs.
- **PostgreSQL (RDS)** as staging area and dimensional data warehouse.

## Data Ingestion (AWS Lambda)

Three Lambda functions implement the ingestion layer:

- API_Air_Quality
- API_Earthquakes
- API_Fires

Each function:
- Connects to an external API.
- Handles pagination and basic error handling.
- Stores raw responses without transformation.
- Ensures reproducibility by preserving original source data.

These functions represent the **entry point** of the data lake.

## Data Transformation (AWS Glue)

### 1. "dw-clean_staging"
Responsible for transforming raw source data into standardized staging tables.

Key characteristics:
- Data cleaning and normalization (timestamps, units, formats).
- Validation of basic constraints (e.g., non-null keys, valid ranges).
- All transformations executed **inside PostgreSQL** using SQL.
- Glue is used as an orchestration layer to ensure traceability.

### 2. "dw-load_rds"
Responsible for loading the **dimensional data warehouse**.

This job:
- Transforms staging data into fact and dimension tables.
- Implements surrogate keys and star-schema logic.
- Ensures idempotent loads where applicable.
- Represents the **Silver → Gold** transition.

## Data Quality Assurance (AWS Glue: "dw-data_quality")

A dedicated Glue job implements a structured data-quality framework.

Validated dimensions:
- **Completeness**
- **Accuracy**
- **Consistency**
- **Timeliness**

Results are logged in: dw.data_quality_log


This enables:
- Full auditability
- Post-run inspection
- Transparency for academic evaluation

## Research Question 3 – Operational Exceedance Risk (AWS Lambda: "RQ3")

To address **RQ3**:

> *Can a simple score be derived to estimate the probability that air quality will exceed WHO limits within 72 hours after an event?*

A dedicated Lambda function ("RQ3") was implemented.

### Function Logic

- Securely connects to the PostgreSQL database.
- Retrieves:
  - Last **90 days** of air-quality measurements.
  - Satellite-based fire detections.
- Aggregates both datasets to **daily resolution**.
- Merges them into a unified time series.

### Modeling Approach

- A **simple linear time-series regression** is trained to predict next-day AQI.
- Predictors:
  - Previous-day AQI
  - Concurrent fire intensity
- Training implemented **explicitly in Python** using stochastic gradient descent.
- No opaque external ML libraries are used to ensure:
  - Transparency
  - Reproducibility
  - Interpretability

The model output is used to derive a **72-hour forward exceedance-risk indicator**.

## Reproducibility and Transparency

- All transformations are SQL-based and executed in PostgreSQL.
- Glue jobs act as orchestration and logging layers.
- Lambda functions are self-contained and version-controlled.
- Data-quality results are persisted for auditing.

## How to use this repository

This repository is intended for:
- Code review and academic evaluation.
- Understanding the end-to-end DW/DL architecture.
- Inspecting ingestion, transformation, validation, and analytics logic.

Deployment requires:
- AWS account with Lambda, Glue, and RDS.
- Proper environment variables and the AWS Identity and Access Management roles (not included here).

## Authors

- Christian Steinemann, christian.steinemann@stud.hslu.ch
- Mattia Bettoja, mattia.bettoja@stud.hslu.ch
- Tobias Schöpfer, tobias.schoepfer@stud.hslu.ch

All team members contributed equally across ingestion, transformation, modeling, and documentation.

## Examiners

- Luis Terán, luis.teran@hslu.ch
- José Mancera, jose.mancera@hslu.ch
- Jhonny Pincay, jhonny.pincaynieves@hslu.ch

