from typing import List, Dict
from pyspark.sql.types import StructType
import json
from schemas.rule_schema import RuleSchema

class ConfigLoader:
    @staticmethod
    def load_rules(schema_path: str) -> StructType:
        """Load schema definition from JSON file"""
        with open(schema_path, "r") as file:
            return StructType.fromJson(json.load(file))
    
    @staticmethod
    def validate_rules(rules: List[Dict]):
        """Validate rules configuation against schema"""
        RuleSchema.validate(rules)