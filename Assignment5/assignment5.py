from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StringType, ArrayType, StructField
from pyspark.sql.functions import col, avg
from Bio import SeqIO
from io import StringIO
#from Bio.SeqFeature import BeforePosition, AfterPosition
from os import getcwd
import json

FILE = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2.genomic.gbff"
#FILE = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.wgs_mstr.gbff"
#FILE = "minini.gbff"
# Initialize Spark session
spark = (
    SparkSession.builder
    .master("local[16]")
    .config("spark.executor.memory","64g")
    .config('spark.driver.memory','64g')
    .getOrCreate()
)
spark.conf.set("spark.task.maxBroadcastSize", "2m")

# Schema for the features DataFrame
feature_schema = StructType([
    StructField("accession", StringType(), True),
    StructField("type", StringType(), True),
    StructField("location_start", StringType(), True),
    StructField("location_end", StringType(), True),
    StructField("organism", StringType(), True)
])

features = ["CDS", "ncRNA", "rRNA", "gene"]

with open(FILE, 'r') as file:
    file_object = file.read()

    records = file_object.split("//\n")
    # Re-atach // at the end for Biopython to recognize a record
    records = [record + "//" for record in records if record.strip()]
    #print(records[0:2])

def bio_parser(record_batch):
    # Parse GBFF file``
    for rec in SeqIO.parse(StringIO(record_batch), format="genbank"):
        feature_record_collection = []
        for feature in rec.features:
            feature_record = {
                "accession": rec.id,
                "type": feature.type,
                "location_start": str(feature.location.start),
                "location_end": str(feature.location.end),
                "organism": rec.annotations.get("organism", "")
            }
            feature_record_collection.append(feature_record)
        return feature_record_collection

# flatmap to unpack the feature_records lists outputs
parallellize_bio_parser = spark.sparkContext.parallelize(records, 16).flatMap(lambda record: bio_parser(record))
# Each row has a record id that holds one feature
feature_records = parallellize_bio_parser.collect()
df_features = spark.createDataFrame(feature_records, schema=feature_schema)
df_features.show()

def question1(frame):
    """
    Hoeveel "features" heeft een Archaea genoom gemiddeld?
    """
    print("Question 1: How much features does an Archaea genome have on average?")
    frame = (
        frame
        .filter(~col("location_start").startswith("<"))
        .filter(~col("location_end").startswith(">"))
        .filter(col("type").isin(features))
    )
    frame.show()
    avg_feature_count = frame.groupBy("accession").count().agg(avg("count")).collect()[0][0]
    print(f"An Archaea genome has {avg_feature_count} features on average")

#question1(df_features)
#Hoe is de verhouding tussen coding en non-coding features? (Deel coding door non-coding totalen).
#Wat zijn de minimum en maximum aantal eiwitten van alle organismen in het file?
#Verwijder alle non-coding (RNA) features en schrijf dit weg als een apart DataFrame (Spark format).
#Wat is de gemiddelde lengte van een feature ?