"""
Initialization file for the Data Quality Framework package.
This file sets up the package structure and imports necessary modules.
"""

from pyspark.sql import SparkSession

print("Initializing Data Quality Framework...")
spark = SparkSession.builder.appName("DataQualityFramework").getOrCreate()

