
## End-to-End AWS Lakehouse with Airflow, Glue, dbt & Athena

This project demonstrates a production-style AWS lakehouse architecture built end to end using managed, serverless AWS services and modern analytics engineering practices.

The pipeline ingests raw data into Amazon S3, transforms and curates it with AWS Glue (PySpark), models analytics-ready tables using dbt on Athena, enforces data quality through automated tests, and orchestrates everything with Apache Airflow.

## Architecture Overview
![Logo](https://github.com/jamesaje/retail-lakehouse-aws/blob/main/proj_architecture.png)


### Orchestration & Operations
- Apache Airflow (Docker-based, MWAA-style)
- Parameterised monthly runs (Year / Month)
- Automated retries, polling, and failure handling

### Governance & Quality
- Partitioned datasets (year/month)
- Incremental models
- dbt tests as quality gates
- IAM-based access control

### Tech Stack

**Storage:** Amazon S3 (Raw / Cleaned / Curated zones)

**Processing:** AWS Glue (PySpark, serverless ELT)

**Metadata:** AWS Glue Data Catalog

**Query Engine:** Amazon Athena

**Transformation:** dbt (Athena adapter)

**Orchestration:** Apache Airflow (Docker, LocalExecutor)

**Data Quality:** dbt tests

**IaC:** Terraform

**Language:** SQL, PySpark, Python


### Data Flow Details
1. Raw Ingestion (S3 – Raw Zone)
     - Raw NYC Taxi trip data stored in S3
     - Data retained in original structure for traceability
     - No transformations applied at this stage

2. ELT with AWS Glue
     - Glue job written in PySpark
     - Parameterised by YEAR and MONTH
     - Reads raw data and:
          - Standardises column names
          - Casts data types
          - Applies basic data quality filters
          - Writes Parquet output partitioned by year/month
      - Output written to S3 Cleaned Zone

3. Metadata & Discovery
     - AWS Glue Crawler updates the Data Catalog
     - Athena automatically discovers new partitions

4. Analytics Engineering with dbt
     - dbt models run directly on Athena
     - Layered modeling approach:
         - Staging models: light cleaning and renaming
         - Mart models: facts and dimensions
     - Incremental fact tables using insert_overwrite
     - Partition-aware modeling for performance and cost efficiency

5. Data Quality & Testing
dbt tests enforce:
     - Not-null constraints
     - Uniqueness (surrogate keys)
     - Accepted values
     - Custom business logic tests (e.g. trip duration validity)

     Failures stop the pipeline and surface immediately in Airflow.

6. Orchestration with Airflow
The DAG performs:
     - Start Glue job (parameterised)
     - Poll until completion
     - Run Glue Crawler
     - Poll crawler status
     - Execute dbt run
     - Execute dbt test (quality gate)
