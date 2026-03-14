import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

#initialise glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# READ CLEANED DATA FROM GLUE CATALOG
# -----------------------------
df = spark.table("healthcare_cleaned_db.diabetic_cleaned_job_05feb2026_1770293408786")

df_clean = (
    df
    .withColumn(
        "medical_specialty",
        F.when(
            (F.col("medical_specialty").isNull()) | (F.col("medical_specialty") == "?"),
            F.lit("Unknown")
        ).otherwise(F.trim(F.col("medical_specialty")))
    )
    .withColumn(
        "time_in_hospital",
        F.col("time_in_hospital").cast("double")
    )
      .withColumn(
        "admission_type_id",
        F.col("admission_type_id").cast("int")
    )
)

# -----------------------------
# CURATED KPI: AVG LENGTH OF STAY BY DEPARTMENT
# -----------------------------
avg_los_df = (
    df_clean.groupBy("medical_specialty")
      .agg(
          F.round(F.avg("time_in_hospital"), 2).alias("avg_length_of_stay_days"),
          F.count("*").alias("total_visits")
      )
      .orderBy(F.desc("total_visits"))
)

# WRITE CURATED DATA TO S3
# -----------------------------
output_path = "s3://healthcare-ops-analytics-ria/curated/avg_length_of_stay_by_department/"

(
    avg_los_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

# -----------------------------
# KPI 2: PATIENT VOLUME BY ADMISSION TYPE
# -----------------------------
# -----------------------------
# ADMISSION TYPE LOOKUP (DIMENSION)
# -----------------------------
admission_type_mapping = spark.createDataFrame(
    [
        (1, "Emergency"),
        (2, "Urgent"),
        (3, "Elective"),
        (4, "Newborn"),
        (5, "Trauma Center"),
        (6, "Not Available"),
        (7, "Not Mapped"),
        (8, "Unknown")
    ],
    ["admission_type_id", "admission_type"]
)



# -----------------------------
# KPI 2: PATIENT VOLUME BY ADMISSION TYPE (WITH LABELS)
# -----------------------------
admission_volume_df = (
    df_clean
        .join(admission_type_mapping, on="admission_type_id", how="left")
        .groupBy("admission_type")
        .agg(F.count("*").alias("total_visits"))
        .orderBy(F.desc("total_visits"))
)

# -----------------------------
# BUCKET RARE ADMISSION TYPES
# -----------------------------
admission_volume_bucketed_df = (
    admission_volume_df
        .withColumn(
            "admission_type_bucketed",
            F.when(F.col("total_visits") < 500, F.lit("Other"))
             .otherwise(F.col("admission_type"))
        )
        .groupBy("admission_type_bucketed")
        .agg(F.sum("total_visits").alias("total_visits"))
        .orderBy(F.desc("total_visits"))
)

# -----------------------------
# WRITE KPI 2 TO S3
# -----------------------------
admission_output_path = (
    "s3://healthcare-ops-analytics-ria/curated/patient_volume_by_admission_type/"
)

(
    admission_volume_bucketed_df
    .write
    .mode("overwrite")
    .parquet(admission_output_path)
)


job.commit()