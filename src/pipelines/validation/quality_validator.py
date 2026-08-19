from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityValidationError(ValueError):
    """
    Raised when a DataFrame fails one or more quality validation checks
    """
    pass

class QualityValidator:
    """
    Performs data quality validation checks include
    Empty dataset
    Required values
    Blank strings
    Duplicate records
    NaN 
    """

    def __init__(self, required_columns:List[str], amount_columns:List[str]):

        self.required_columns= required_columns
        self.amount_columns= amount_columns
        #self.duplicate_keys= duplicate_keys


    def validate(self, df:DataFrame):

        self._validate_not_empty(df)
        self._validate_required_values(df)
        self._validate_blank_strings(df)
        #self._validate_duplicate_rows(df)
        self._validate_numeric_values(df)


    def _validate_not_empty(self, df:DataFrame):
        if df.limit(1).count()==0:
            raise QualityValidationError("DataFrame is empty.")
        

    def _validate_required_values(self, df:DataFrame):
        for column in self.required_columns:
            count= df.filter(F.col(column).isNull()).limit(1).count()

            if count>0:
                raise QualityValidationError(f"Column '{column}' contains Null values")
            
    
    def _validate_blank_strings(self, df:DataFrame):
        for column in self.required_columns:
            count= df.filter(F.col(column) =="").limit(1).count()

            if count>0: 
                raise QualityValidationError(f"Column '{column}' contains blank strings")
            

    """def _validate_duplicate_rows(self, df:DataFrame):

        duplicates= (df.groupBy(*self.duplicate_keys).count().filter(F.col("count")>1).limit(1).count())

        if duplicates:
            raise QualityValidationError(f"Duplicate records found using keys:{self.duplicate_keys}")
    """
        

    def _validate_numeric_values(self, df:DataFrame):
        for column in self.amount_columns:
            invalid= df.filter(F.isnan(column) | F.col(column).isin(float('inf'), float('-inf'))).limit(1).count()
            if invalid:
                raise QualityValidationError(f"Invalid numeric values detected in '{column}'")


