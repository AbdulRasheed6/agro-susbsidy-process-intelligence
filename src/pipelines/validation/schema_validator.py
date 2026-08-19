from typing import Dict, List
from pyspark.sql import DataFrame
from collections import Counter

class SchemaValidationError(ValueError):
    """
    Raised when a DataFrame schema does not match the expected canonical schema
    """
    pass 

class SchemaValidator:
    """
    Validates that a transformed dataframe conforms to the canonical Silver schema.
    This validator only checks schema , it does not inspect data quality or business rules 
    """

    def __init__(self, expected_schema:Dict[str, str]):

        self.expected_schema= expected_schema
        self.expected_columns= list(expected_schema.keys())
        

        
    def validate(self, df:DataFrame) -> None:
        self._validate_missing_columns(df)
        self._validate_duplicate_column(df)
        self._validate_extra_columns(df)
        self._validate_column_order(df)
        self._validate_column_types(df)

            
            #if self.expected_types:
            #    self._validate_column_types(df)
        
    def _validate_missing_columns(self, df:DataFrame):
        actual_columns= set(df.columns)
        missing = [column for column in self.expected_columns if column not in actual_columns]

        if missing:
            raise SchemaValidationError(f"Missing required columns: {missing}")
        
    def _validate_duplicate_column(self, df:DataFrame):
        counts=  Counter(df.columns)
        duplicates= [column for column, count in counts.items() if count>1]

        if duplicates:
            raise SchemaValidationError(f"Duplicate columns detected {duplicates}")
        
    def _validate_extra_columns(self, df:DataFrame):
        expected= set(self.expected_columns)
        extra= [column for column in df.columns if column not in expected]

        if extra:
            raise SchemaValidationError(f"Unexpected columns detected. {extra}")
        
    def _validate_column_order(self, df:DataFrame):
        if df.columns != self.expected_columns:
            raise SchemaValidationError(
                "Column order mismatch. \n\n"
                f"Expected: \n{self.expected_columns}\n\n"
                f"Recieved: \n{df.columns}"
                )
    def _validate_column_types(self, df:DataFrame):
        actual_types= dict(df.dtypes)

        mismatches = []
        for column , expected_type in self.expected_schema.items():
            actual_type= actual_types.get(column)

            if actual_type != expected_type:
                mismatches.append(
                    {
                        "column":column,
                        "expecteed": expected_type,
                        "actual": actual_type
                    }
                )
            if mismatches:
                raise SchemaValidationError(f"Column datatype mismatch: {mismatches}")