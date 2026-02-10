# Healthcare Operations Analytics on AWS (End-to-End ETL Pipeline)

## Project Overview

This project implements an end-to-end cloud-based ETL and analytics pipeline on AWS to analyze hospital operations data. The pipeline transforms raw healthcare encounter data into curated, business-ready datasets and interactive visualizations to support operational decision-making.

It covers the full analytics lifecycle,from raw ingestion to dashboard consumption, using managed AWS services.

Checkout my medium article where I do a walkthrough of my project in detail.

## Business Problem

Hospitals generate large volumes of operational data, but decision-makers often lack clear insights into:

- Which departments handle the highest patient volumes
- How average length of stay varies across departments
- How patient admissions are distributed across admission types (Emergency, Elective, Urgent)

Without a structured analytics pipeline:

- Raw data is difficult to analyze
- Metrics become inconsistent
- Dashboards can produce misleading results

## Objective

Build a scalable analytics pipeline that:

- Cleans and standardizes raw hospital encounter data
- Produces curated datasets aligned with business metrics
- Enables interactive analysis through Amazon QuickSight

## Dataset

- **Source:** UCI Machine Learning Repository
- **Dataset:** Diabetes 130 US Hospitals Dataset
- **Records:** ~100,000 hospital encounters
- **Format:** CSV
- **Link:** https://archive.ics.uci.edu/ml/datasets/diabetes

The dataset includes patient encounter details such as diagnoses, admission types, medical specialties, and hospital length of stay.

## Architecture Overview

The architecture follows a layered data lake design with clear separation between raw, cleaned, and curated data.

### Architecture Flow

```text
Raw CSV Data
     |
     v
Amazon S3 (Raw Layer)
     |
     v
AWS Glue DataBrew (Data profiling and cleaning)
     |
     v
Amazon S3 (Cleaned Layer)
     |
     v
AWS Glue PySpark Jobs (Curated transformations and aggregations)
     |
     v
Amazon S3 (Curated Layer)
     |
     v
AWS Glue Crawlers (Schema discovery and cataloging)
     |
     v
Amazon Athena (SQL analytics and validation)
     |
     v
Amazon QuickSight (Dashboards and visualizations)
```

## ETL Workflow

### Step 1: Raw Data Ingestion

Raw CSV files are uploaded to Amazon S3 in a dedicated **raw layer**. This layer acts as the immutable source of truth and allows reprocessing when business logic changes.

### Step 2: Data Profiling and Cleaning

AWS Glue DataBrew is used to profile and clean data. Key actions include:

- Identifying missing and invalid values
- Handling ambiguous categorical values (e.g., `?`)
- Dropping irrelevant or high-cardinality columns
- Standardizing categorical fields such as medical specialty

Cleaned output is written back to Amazon S3 in a **cleaned layer**.

### Step 3: Schema Registration for Cleaned Data

An AWS Glue Crawler runs on the cleaned layer to infer schema and register tables in the Glue Data Catalog, enabling SQL access through Athena.

### Step 4: Curated Transformations Using Glue PySpark

Custom AWS Glue PySpark jobs generate curated analytics tables, including:

- Total patient visits by department
- Average length of stay by medical specialty
- Mapping numeric admission type IDs to business-readable categories

Curated outputs are stored in Amazon S3 in **Parquet** format for query efficiency.

### Step 5: Schema Registration for Curated Data

Glue Crawlers run on curated S3 paths to update the Data Catalog schemas automatically.

### Step 6: Analytics and Validation Using Athena

Athena SQL queries are used to validate curated datasets and confirm transformation logic before visualization.

### Step 7: Visualization in Amazon QuickSight

QuickSight dashboards are built on Athena datasets. Visuals are organized into multiple sheets to separate department-level KPIs from admission-type distribution views.

## Key Metrics and Visualizations

The project tracks the following business metrics:

- Total patient visits by department
- Average length of stay by department
- Patient volume by admission type

Dashboard screenshots are available in the `dashboards/` folder.

## Challenges Encountered

### Data Quality Issues

Several columns contained missing or inconsistent values. For example, `medical_specialty` included ambiguous placeholders and nulls. Numeric IDs also needed mapping to human-readable categories, requiring multiple reprocessing iterations.

### IAM Permission Errors

Initial Glue jobs failed due to insufficient IAM permissions for writing curated outputs to S3. This was resolved by refining IAM role policies.

### Silent Spark Join Failures

Some transformations produced empty outputs due to datatype mismatches in joins. These failures were resolved by enforcing consistent schemas.

### QuickSight Aggregation Issues

Early dashboard versions showed misleading proportions due to incorrect aggregation settings. This was corrected by explicitly setting measures and aggregation levels.

## Future Improvements

Potential enhancements:

- Orchestrate Glue jobs and crawlers with AWS Step Functions
- Parameterize Glue jobs to reduce repeated code edits
- Add automated data quality checks
- Enable event-driven processing with S3 + Lambda
- Version curated datasets for auditability

## Tools and Technologies

- Amazon S3
- AWS Glue DataBrew
- AWS Glue PySpark
- AWS Glue Crawlers and Data Catalog
- Amazon Athena
- Amazon QuickSight

## Repository Structure
```text
Healthcare-Operations-Analytics-on-AWS-End-to-End-ETL-Pipeline-/
├── README.md
├── architecture/
│   └── readit.txt
├── dashboard/
│   ├── healthcare dashboard.png
│   └── placeholder.txt
├── databrew_cleaning/
│   ├── clean.txt
│   └── data_cleaning.png
├── pyspark/
│   ├── 1.initialise_job.png
│   ├── 2.read_cleaned_data.png
│   ├── 3.avg_length_of_stay_by_department.png
│   ├── 4.dimension_mapping.png
│   ├── 5.patient_volume_by_admission_type.png
│   ├── 6.commit_job.png
│   └── readd.txt
└── storage/
    ├── s3_layers.png
    └── ss.txt
```

```

## Notes

- QuickSight was enabled temporarily for visualization and then disabled after screenshots were captured to control cost.
- The dataset is publicly available and therefore not included directly in this repository.
