"""
Cannonical silver layer schema.

Every bronze dataset, regardless of country or year ,  must be transformed into this schema
"""
Silver_Schema = {
    "beneficiary": "string",
    "municipality": "string",
    "country": "string",
    "year": "string",
    "intervention_code": "string",
    "total_eagf_income_support": "double",
    "total_eafrd_income_support": "double",
    "national_cofunding_amount": "double",
    "silver_created_at": "timestamp"


}



SILVER_TEXT_COLUMNS= [
    "beneficiary",
    "municipality",
    "intervention_code",
    "country",
    "year"

]

SILVER_AMOUNT_COLUMNS= [
    "total_eagf_income_support",
    "total_eafrd_income_support",
    "national_cofunding_amount"
]