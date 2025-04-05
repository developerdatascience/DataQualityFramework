from engine import DataQualityEngine
from reports import HTMLReportGenerator
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load data
transactions_df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True)

# Register temp views for referential checks
products_df.createOrReplaceTempView("products")

# Run data quality checks
engine = DataQualityEngine(transactions_df)
good_df, bad_df = engine.apply_rules("frameworkDQ/rules/config.json")


# engine.save_rejected(base_path="rejected_records")

# print("Quality stats: ",engine.get_quality_stats())
# print("Failed Rule Summmary: ", engine.get_failed_rules_summary().show(truncate=False))
report_gen = HTMLReportGenerator(engine)
report_gen.generate_report("quality_report.html")

# Show results
# print(f"Passed records: {good_df.count()}")
# print(f"Rejected records: {bad_df.count()}")
# bad_df.show(truncate=False)


spark.stop()