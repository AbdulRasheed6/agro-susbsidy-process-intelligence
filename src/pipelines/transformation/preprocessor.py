"""
Dataset-specific preprocessing.  
These are only used when the Bronze dataset needs structural fixing before transformation
"""

import pyspark.sql.functions as F

def no_preprocessing(df):
    """
    Default
    """
    return df

def second_row_header(df):

    """
    Used only for datasets whose real headers are stored in 2 row
    """

    legitimate= [
        "source_country",
        "source_year",
        "ingested_at"
    ]

    sample= df.take(2)[1]
    new_columns= []
    for idx, old in enumerate(df.columns):
        if old in legitimate:
            new_columns.append(old)
        else:
            new_columns.append(str(sample[idx]))
    rename_map= dict(zip(df.columns, new_columns))

    df= df.withColumnsRenamed(rename_map)
    first_data_column= [c for c in new_columns if c not in legitimate][0]
    return df.filter(F.col(first_data_column) != first_data_column)
    
