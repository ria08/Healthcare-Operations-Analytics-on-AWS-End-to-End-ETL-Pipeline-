import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─────────────────────────────────────────────
# INITIALISE GLUE JOB
# ─────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ─────────────────────────────────────────────
# STEP 1 — READ CLEANED DATA FROM GLUE CATALOG
# Same source your existing KPI job already uses
# ─────────────────────────────────────────────
df_clean = spark.table("healthcare_cleaned_db.diabetic_cleaned_job_05feb2026_1770293408786")

df_clean = (
    df_clean
    .withColumn(
        "medical_specialty",
        F.when(
            (F.col("medical_specialty").isNull()) | (F.col("medical_specialty") == "?"),
            F.lit("Unknown")
        ).otherwise(F.trim(F.col("medical_specialty")))
    )
    .withColumn("time_in_hospital", F.col("time_in_hospital").cast("double"))
    .withColumn("admission_type_id", F.col("admission_type_id").cast("int"))
)


# ─────────────────────────────────────────────
# STEP 2 — READ RAW DATA FROM S3
# Needed for age + insulin columns (dropped during DataBrew cleaning)
# Join back on encounter_id
# ─────────────────────────────────────────────
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3://healthcare-ops-analytics-ria/raw/diabetic_data/diabetic_data.csv")
    .select("encounter_id", "age", "insulin")
    .withColumn("encounter_id", F.col("encounter_id").cast("long"))
)

df_clean = df_clean.withColumn("encounter_id", F.col("encounter_id").cast("long"))

# Left join — keeps all cleaned records, adds age + insulin where available
df = df_clean.join(df_raw, on="encounter_id", how="left")


# ─────────────────────────────────────────────
# KPI 3 — READMISSION BY DEPARTMENT
# Metrics: total visits, avg LOS, 30-day readmit rate, efficiency score
# Filter: departments with >= 200 visits (removes noise from tiny specialties)
# ─────────────────────────────────────────────

dept_df = (
    df
    .filter(F.col("medical_specialty") != "Unknown")
    .groupBy("medical_specialty")
    .agg(
        F.count("*").alias("total_visits"),
        F.round(F.avg("time_in_hospital"), 2).alias("avg_los_days"),
        F.sum(
            F.when(F.col("readmitted") == "<30", 1).otherwise(0)
        ).alias("readmit_30_day_count"),
        F.sum(
            F.when(F.col("readmitted") != "NO", 1).otherwise(0)
        ).alias("any_readmit_count"),
    )
    .withColumn(
        "readmit_30_day_rate_pct",
        F.round((F.col("readmit_30_day_count") / F.col("total_visits")) * 100, 1)
    )
    .withColumn(
        "any_readmit_rate_pct",
        F.round((F.col("any_readmit_count") / F.col("total_visits")) * 100, 1)
    )
    # Apply minimum volume filter
    .filter(F.col("total_visits") >= 200)
)

# ── Efficiency Score ──────────────────────────────────────────────────────────
# Combines avg LOS + 30-day readmission rate into a single 0-100 score
# Higher score = more efficient (lower LOS and lower readmission)
# Formula: 1 - (0.5 * normalised_LOS + 0.5 * normalised_readmit_rate)
# Normalisation: min-max scaling across all qualifying departments

los_min    = dept_df.agg(F.min("avg_los_days")).collect()[0][0]
los_max    = dept_df.agg(F.max("avg_los_days")).collect()[0][0]
rdm_min    = dept_df.agg(F.min("readmit_30_day_rate_pct")).collect()[0][0]
rdm_max    = dept_df.agg(F.max("readmit_30_day_rate_pct")).collect()[0][0]

dept_df = (
    dept_df
    .withColumn(
        "los_normalised",
        (F.col("avg_los_days") - los_min) / (los_max - los_min)
    )
    .withColumn(
        "readmit_normalised",
        (F.col("readmit_30_day_rate_pct") - rdm_min) / (rdm_max - rdm_min)
    )
    .withColumn(
        "efficiency_score",
        F.round(
            (1 - (F.col("los_normalised") * 0.5 + F.col("readmit_normalised") * 0.5)) * 100,
            1
        )
    )
    # Drop intermediate normalisation columns — not needed in output
    .drop("los_normalised", "readmit_normalised")
    .orderBy(F.desc("total_visits"))
)

# Write KPI 3 to S3
dept_output_path = "s3://healthcare-ops-analytics-ria/curated/readmission_by_department/"
(
    dept_df
    .write
    .mode("overwrite")
    .parquet(dept_output_path)
)


# ─────────────────────────────────────────────
# KPI 4 — READMISSION RISK PROFILE
# Breaks down 30-day readmission rate by age band and insulin status
# Answers: who is most at risk of coming back within 30 days?
# ─────────────────────────────────────────────

# Clean age — should already be banded ([0-10) etc) but trim whitespace
df_age = df.withColumn("age", F.trim(F.col("age")))

# Clean insulin — standardise nulls
df_age = df_age.withColumn(
    "insulin",
    F.when(F.col("insulin").isNull(), F.lit("Unknown"))
     .otherwise(F.trim(F.col("insulin")))
)

risk_df = (
    df_age
    .groupBy("age", "insulin")
    .agg(
        F.count("*").alias("total_patients"),
        F.sum(
            F.when(F.col("readmitted") == "<30", 1).otherwise(0)
        ).alias("readmit_30_day_count"),
        F.sum(
            F.when(F.col("readmitted") != "NO", 1).otherwise(0)
        ).alias("any_readmit_count"),
    )
    .withColumn(
        "readmit_30_day_rate_pct",
        F.round((F.col("readmit_30_day_count") / F.col("total_patients")) * 100, 1)
    )
    .withColumn(
        "any_readmit_rate_pct",
        F.round((F.col("any_readmit_count") / F.col("total_patients")) * 100, 1)
    )
    # Remove very small cohorts (< 30 patients) — rates unreliable at small n
    .filter(F.col("total_patients") >= 30)
    .orderBy("age", "insulin")
)

# Also compute age-only summary (simpler view for dashboard)
age_summary_df = (
    df_age
    .groupBy("age")
    .agg(
        F.count("*").alias("total_patients"),
        F.sum(
            F.when(F.col("readmitted") == "<30", 1).otherwise(0)
        ).alias("readmit_30_day_count"),
    )
    .withColumn(
        "readmit_30_day_rate_pct",
        F.round((F.col("readmit_30_day_count") / F.col("total_patients")) * 100, 1)
    )
    .orderBy("age")
)

# Write KPI 4a — full risk profile (age x insulin)
risk_output_path = "s3://healthcare-ops-analytics-ria/curated/readmission_risk_profile/"
(
    risk_df
    .write
    .mode("overwrite")
    .parquet(risk_output_path)
)

# Write KPI 4b — age summary only (cleaner for one QuickSight visual)
age_output_path = "s3://healthcare-ops-analytics-ria/curated/readmission_by_age/"
(
    age_summary_df
    .write
    .mode("overwrite")
    .parquet(age_output_path)
)


# ─────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────
job.commit()
