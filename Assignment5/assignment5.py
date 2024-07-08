#!/usr/bin/env python3

"""
Assignment 5 implements spark functionality to create a dataframe from a BGFF file and processes
records accordingly with the questions
"""

# META DATA
__author__ = "LT Stein"
__date__ = "08-07-2024"

# IMPORTS
import os
import shutil

from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    BooleanType,
    IntegerType,
    StructField,
)
from pyspark.sql.functions import (
    col,
    avg,
    count,
    expr,
    min as min_ps,
    max as max_ps,
)
from Bio.SeqFeature import CompoundLocation
from Bio import SeqIO

FILENAME = "archaea.2.genomic.gbff"
FILE = (
    "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/" + FILENAME
)

# Initialize Spark session
spark = (
    SparkSession.builder.master("local[16]")
    .config("spark.executor.memory", "64g")
    .config("spark.driver.memory", "64g")
    .getOrCreate()
)
spark.conf.set("spark.task.maxBroadcastSize", "2m")

sc = spark.sparkContext
sc.setLogLevel("OFF")

# Schema for the features DataFrame
feature_schema = StructType(
    [
        StructField("accession", StringType(), True),
        StructField("type", StringType(), True),
        StructField("location_start", IntegerType(), True),
        StructField("location_end", IntegerType(), True),
        StructField("organism", StringType(), True),
        StructField("is_protein", BooleanType(), True),
    ]
)

features = ["ncRNA", "rRNA", "gene", "propeptide", "CDS"]

with open(FILE, mode="r", encoding="UTF-8") as file:
    file_object = file.read()

    records = file_object.split("//\n")
    # Re-atach // at the end for Biopython to recognize a record
    records = [record + "//" for record in records if record.strip()]


def bio_parser(record_batch):
    """
    Parse GBFF file
    """
    is_protein = False
    for rec in SeqIO.parse(StringIO(record_batch), format="genbank"):
        feature_record_collection = []
        for feature in rec.features:
            if feature.type == "CDS":
                if "protein_id" in feature.qualifiers:
                    is_protein = True
            # location: join{[262164:262202](-), [262046:262082](-)}
            start = int(feature.location.start)
            end = int(feature.location.end)
            if isinstance(feature.location, CompoundLocation):
                start = int(feature.location.parts[0].start)
                end = int(feature.location.parts[-1].end)
            feature_record = {
                "accession": rec.id,
                "type": feature.type,
                "location_start": start,
                "location_end": end,
                "organism": rec.annotations.get("organism", ""),
                "is_protein": is_protein,
            }
            feature_record_collection.append(feature_record)
            is_protein = False
        return feature_record_collection


# flatmap to unpack the feature_records lists outputs
parallellize_bio_parser = spark.sparkContext.parallelize(records, 16).flatMap(
    lambda record: bio_parser(record)
)
# Each row has a record id that holds one feature
feature_records = parallellize_bio_parser.collect()
df_features = spark.createDataFrame(feature_records, schema=feature_schema)
df_features = (
    df_features.filter(~col("location_start").startswith("<"))
    .filter(~col("location_end").startswith(">"))
    .filter(col("type").isin(features))
)

# Keep records without cryptic genes i.e. not both CDS and gene present
all_genes = df_features.filter(col("type") == "gene").alias("genes")
all_cds = df_features.filter(col("type") == "CDS").alias("CDS")
# Join takes all corresponding genes with cdses based on the conditions
# left_anti reverts those conditions to keep the cryptic genes.
# Has no CDS-gene paired records
crgenes_records_df = all_genes.join(
    all_cds,
    on=[
        all_genes.accession == all_cds.accession,
        all_genes.location_start == all_cds.location_start,
        all_genes.location_end == all_cds.location_end,
    ],
    how="left_anti",
)


def question1(frame):
    """
    Hoeveel "features" heeft een Archaea genoom gemiddeld?
    """
    avg_feature_count = (
        frame.groupBy("accession").count().agg(avg("count")).collect()[0][0]
    )
    print("Question 1: How many features does an Archaea genome have on average?")
    print(f"An Archaea genome has {round(avg_feature_count)} features on average")


def question2(frame, crgenes_records):
    """
    What is the ratio between coding and non-coding features? (Divide coding by non-coding totals)?
    """
    # Keep all rows without cryptic genes
    frame_without_cryptic_genes = frame.join(
        crgenes_records, on="accession", how="left_anti"
    )
    # Count all non-coding (ncRNA, rRNA) features inside
    remaining_non_coding_count = frame_without_cryptic_genes.filter(
        col("type").isin(features[:2])
    ).count()
    # Count all coding (propeptides, genes, CDS) features inside
    coding_count = frame_without_cryptic_genes.filter(
        col("type").isin(features[2:])
    ).count()
    non_coding_count = remaining_non_coding_count + crgenes_records.count()

    print(
        "Question2: What is the ratio between coding and non-coding features? (coding / non-coding totals)"
    )
    print(
        f"That ratio of coding to non-coding features is {round((coding_count / non_coding_count), 3)}."
    )


def question3(frame):
    """
    What are the minimum and maximum number of proteins of all organisms in the file?
    """
    is_true = frame.filter((col("is_protein") == True) & (col("type") == "CDS"))
    CDS_proteins = is_true.groupBy("organism").agg(count("*").alias("count"))
    # Extremes of protein counts among all organisms
    min_max_proteins = CDS_proteins.agg(
        min_ps("count").alias("minimum"), max_ps("count").alias("maximum")
    )
    print(
        "Question3: What are the minimum and maximum number of proteins of all organisms in the file?"
    )
    print(
        f"The minimum is: {min_max_proteins.first()['minimum']} and the maximum is: {min_max_proteins.first()['maximum']}"
    )


def question4(frame, crgenes_records):
    """
    Remove all non-coding (RNA) features and write this as a separate DataFrame (Spark format)
    """
    coding_frame = (
        frame.filter(~(col("type") == "ncRNA"))
        .filter(~(col("type") == "rRNA"))
        .join(crgenes_records, on="accession", how="left_anti")
    )
    table_name = "coding_spark_frame"
    # Remove directory if exists, for re-running after the table was written
    if os.path.exists(f"spark-warehouse/{table_name}"):
        shutil.rmtree(f"spark-warehouse/{table_name}")
    coding_frame.write.saveAsTable(table_name)
    print(
        "Question4: Remove all non-coding (RNA) features and write this as a separate DataFrame (Spark format)"
    )
    print(f"Wrote dataframe as table to {table_name}")


def question5(frame):
    """
    What is the average length of a feature?
    """
    average_length = (
        frame.withColumn("length", expr("location_end-location_start"))
        .agg({"length": "avg"})
        .select("avg(length)")
        .collect()[0][0]
    )
    print("Question5: What is the average length of a feature?")
    print(f"That value is {round(average_length)}")


print(f"\nQuestions answered for file: {FILENAME}\n")
question1(df_features)
print("\n")
question2(df_features, crgenes_records_df)
print("\n")
question3(df_features)
print("\n")
question4(df_features, crgenes_records_df)
print("\n")
question5(df_features)

spark.stop()
