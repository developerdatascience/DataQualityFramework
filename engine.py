"""
Data Quality Engine for validating and processing data in a Spark DataFrame.
This module provides functionality to load data quality rules, validate data,
and separate good and bad records based on the rules.
It includes methods for loading rules from a JSON file, applying the rules to the DataFrame,
and generating reports on the validation results.
It uses PySpark for data processing and JSON schema validation for rules.
The DataQualityEngine class is initialized with a DataFrame and provides methods to load rules,
validate data, and separate good and bad records.
The class also includes methods to generate reports on the validation results.
"""

import json
from datetime import datetime
from tqdm import tqdm
from functools import reduce
from typing import List, Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schemas.rule_schema import RuleSchema
from pyspark.sql import SparkSession


import json
from datetime import datetime
from tqdm import tqdm
from functools import reduce
from typing import List, Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schemas.rule_schema import RuleSchema
from pyspark.sql import SparkSession


class DataQualityEngine:
    def __init__(self, df: DataFrame):
        """
        Initialize the DataQualityEngine with a DataFrame.

        :param df: The DataFrame to be used for data quality checks.
        """
        self.original_df = df.cache()  # Cache for performance
        self.spark = df.sparkSession
        self.validated = False
        self.good_df = None
        self.bad_df = None
        self.rule_stats = {}  # Track stats per rule

    def load_rules(self, rules_path: str) -> List[Dict[str, Any]]:
        """
        Load and validate data quality rules from JSON file.

        Args:
            rules_path (str): Path to the JSON rules file

        Returns:
            List[Dict[str, Any]]: List of validated rules
        """
        with open(rules_path, "r") as file:
            rules = json.load(file)
        
        RuleSchema.validate(rules)
        return sorted(rules, key=lambda x: x['priority'])

    @staticmethod
    def parse_duration(duration_str: str) -> float:
        """
        Parse a duration string (e.g., '72 HOURS') into seconds.
        
        Args:
            duration_str: String in format 'number unit' (e.g., '72 HOURS')
        
        Returns:
            Duration in seconds as a float
        
        Raises:
            ValueError: If format is invalid or unit is unsupported
        """
        parts = duration_str.split()
        if len(parts) != 2:
            raise ValueError(f"Invalid duration format: {duration_str}. Expected 'number unit', e.g., '72 HOURS'")
        
        number_str, unit = parts
        try:
            number = float(number_str)
        except ValueError:
            raise ValueError(f"Invalid number in duration: {number_str}")
        
        unit = unit.upper()
        unit_to_seconds = {
            "SECONDS": 1,
            "MINUTES": 60,
            "HOURS": 3600,
            "DAYS": 86400,
        }
        
        if unit not in unit_to_seconds:
            raise ValueError(f"Unsupported unit: {unit}. Supported units: {list(unit_to_seconds.keys())}")
        
        return number * unit_to_seconds[unit]
    

    def _generate_condition(self, rule: Dict) -> F.Column:
        """
        Generate Spark SQL condition based on rule type.

        :param rule: A dictionary containing the rule details.
        :return: Spark Column expression representing the condition
        """
        rule_type = rule['type']
        
        try:
            if rule_type in ['null_check', 'regex_match', 'value_range', 'length_check',
                            'allowed_values', 'date_format']:
                col_name = rule['column']
                col = F.col(col_name)
                
                if rule_type == 'null_check':
                    return col.isNull()
                elif rule_type == 'regex_match':
                    return ~col.rlike(rule['pattern'])
                elif rule_type == 'value_range':
                    min_val = rule.get('min', float('-inf'))
                    max_val = rule.get('max', float('inf'))
                    return (col < min_val) | (col > max_val)
                elif rule_type == 'allowed_values':
                    return ~col.isin(rule['allowed_values'])
                elif rule_type == 'date_format':
                    return F.to_date(col, rule['format']).isNull()
                elif rule_type == 'length_check':
                    col_name = rule['column']
                    col = F.col(col_name)
                    conditions = []
                    if 'min_length' in rule:
                        conditions.append(F.length(col) < rule['min_length'])
                    if 'max_length' in rule:
                        conditions.append(F.length(col) > rule['max_length'])
                    if not conditions:
                        raise ValueError("length_check rule requires at least one of 'min_length' or 'max_length'")
                    return col.isNull() | reduce(lambda a, b: a | b, conditions)

            elif rule_type in ['cross_column', 'completeness_check']:
                columns = rule['columns']
                
                if rule_type == 'cross_column':
                    return F.expr(rule['condition'])
                elif rule_type == 'completeness_check':
                    return reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])

            elif rule_type == 'time_difference':
                columns = rule["columns"]
                if len(columns) != 2:
                    raise ValueError("time_difference rule requires exactly two columns")
                col1, col2 = columns
                max_seconds = self.parse_duration(rule["max_duration"])
                return (F.col(col1).isNull() | 
                        F.col(col2).isNull() | 
                        ((F.unix_timestamp(col2) - F.unix_timestamp(col1)) > max_seconds))

            elif rule_type == 'referential_integrity':
                col = F.col(rule['column'])
                ref_df = self.spark \
                            .table(rule['reference_table']) \
                            .select(rule['reference_column']) \
                            .distinct()
                broadcast_ref = F.broadcast(ref_df)
                return ~col.isin(broadcast_ref.rdd.flatMap(lambda x: x).collect())

            elif rule_type == 'statistical':
                col = F.col(rule['column'])
                if rule['statistic'] == 'z_score':
                    window = Window.partitionBy()  # Global stats
                    mean = F.mean(col).over(window)
                    stddev = F.stddev(col).over(window)
                    z_score = (col - mean) / stddev
                    return F.abs(z_score) > rule['threshold']

            else:
                raise ValueError(f"Unsupported rule type: {rule_type}")

        except KeyError as e:
            raise KeyError(f"Missing required parameter '{e.args[0]}' for {rule_type} rule") from e


    def _handle_unique_check(self, df: DataFrame, rule: Dict) -> DataFrame:
        """
        Handle unique_check rule by pre-computing duplicates.

        :param df: Input DataFrame
        :param rule: Rule dictionary for unique_check
        :return: DataFrame with failed_rules updated for duplicates
        """
        col_name = rule['column']
        # Create a temporary DF with duplicate counts
        dup_counts = (df.groupBy(col_name)
                     .agg(F.count(F.lit(1)).alias("dup_count"))
                     .filter(F.col("dup_count") > 1))
        
        # Join back to original DF and mark duplicates
        return df.join(F.broadcast(dup_counts), col_name, "left_outer").withColumn(
            "failed_rules",
            F.when(
                F.col("dup_count").isNotNull(),
                F.array_union(F.col("failed_rules"), F.array(F.lit(rule['name'])))
            ).otherwise(F.col("failed_rules"))
        ).drop("dup_count")

    def apply_rules(self, rules_path: str) -> Tuple[DataFrame, DataFrame]:
        """
        Apply all quality rules to the dataset.

        :param rules_path: Path to the JSON rules file
        :return: Tuple of (good DataFrame, bad DataFrame)
        """
        rules = self.load_rules(rules_path)
        
        # Initialize with empty array and a unique row identifier
        df = self.original_df.withColumn("row_id", F.monotonically_increasing_id()) \
                            .withColumn("failed_rules", F.array().cast("array<string>"))
        
        with tqdm(total=len(rules), desc="Applying DQ Rules") as pbar:
            for rule in rules:
                rule_name = rule['name']
                if rule['type'] == 'unique_check':
                    df = self._handle_unique_check(df, rule)
                else:
                    condition = self._generate_condition(rule)
                    df = df.withColumn(
                        "failed_rules",
                        F.when(
                            condition,
                            F.array_union(F.col("failed_rules"), F.array(F.lit(rule_name)))
                        ).otherwise(F.col("failed_rules"))
                    )
                
                # Update rule stats
                failed_count = df.filter(F.array_contains(F.col("failed_rules"), rule_name)).count()
                self.rule_stats[rule_name] = {"failed_count": failed_count}
                pbar.update(1)
        
        # Split into good/bad records
        self.bad_df = df.filter(F.size("failed_rules") > 0).drop("row_id")
        self.good_df = df.filter(F.size("failed_rules") == 0).drop("failed_rules", "row_id")
        self.validated = True
        
        return self.good_df, self.bad_df

    def save_rejected(self, base_path: str) -> str:
        """
        Save the rejected records to a parquet file.

        :param base_path: Base directory path for saving
        :return: Path where rejected records were saved
        """
        if not self.validated:
            raise ValueError("Data has not been validated yet. Please run apply_rules() first.")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rejected_path = f"{base_path}/rejected_{timestamp}"
        self.bad_df.write.mode("overwrite").parquet(rejected_path)
        return rejected_path
    
    def get_quality_stats(self) -> Dict[str, Any]:
        """
        Generate statistics about the data quality checks.

        :return: Dictionary containing quality statistics
        """
        if not self.validated:
            raise ValueError("Data has not been validated yet. Please run apply_rules() first.")
        
        total_records = self.original_df.count()
        bad_records = self.bad_df.count()
        
        stats = {
            "total_records": total_records,
            "passed_records": total_records - bad_records,
            "rejected_records": bad_records,
            "quality_percentage": ((total_records - bad_records) / total_records * 100) if total_records > 0 else 0,
            "rule_violations": self.rule_stats
        }
        
        return stats

    def get_failed_rules_summary(self) -> DataFrame:
        """
        Generate a summary of failed rules per record.

        :return: DataFrame with row_id and failed rules
        """
        if not self.validated:
            raise ValueError("Data has not been validated yet. Please run apply_rules() first.")
        
        return self.bad_df.select("order_id", "failed_rules")



        