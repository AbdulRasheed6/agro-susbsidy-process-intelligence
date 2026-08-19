from src.pipelines.transformation.preprocessor import no_preprocessing, second_row_header
from src.pipelines.transformation.numeric_cleaners import cast_only, comma_decimal


DATASET_REGISTRY= {
    "SPAIN_2023":{
        "grain":["beneficiary", "intervention_code", "municipality"],
        "preprocessor": no_preprocessing,
        "columns": {
            "beneficiary":{
                "source":"beneficiario",
                "transform": None
            },
            "municipality":{
                "source":"municipio",
                "transform": None
            },
            "country":{
                "source": "source_country",
                "transform": None
            },
            "year":{
                "source": "source_year",
                "transform": None
            },
            "intervention_code":{
                "source": "medida",
                "transform": None
            },
            "total_eagf_income_support":{
                "source": "feaga",
                "transform": comma_decimal
            },
            "total_eafrd_income_support":{
                "source": "feader",
                "transform": comma_decimal
            },
            "national_cofunding_amount":{
                "source": "importecofin",
                "transform": comma_decimal
            }
        }
    },
        
        
    "SPAIN_2024": {
        "preprocessor": no_preprocessing,
        "grain":["beneficiary", "intervention_code", "municipality"],

        "columns": {
            "beneficiary":{
                "source":"beneficiario",
                "transform": None
            },
            "municipality":{
                "source":"municipio",
                "transform": None
            },
            "country":{
                "source": "source_country",
                "transform": None
            },
            "year":{
                "source": "source_year",
                "transform": None
            },
            "intervention_code":{
                "source": "medida",
                "transform": None
            },
            "total_eagf_income_support":{
                "source": "feaga",
                "transform": comma_decimal
            },
            "total_eafrd_income_support":{
                "source": "feader",
                "transform": comma_decimal
            },
            "national_cofunding_amount":{
                "source": "importecofin",
                "transform": comma_decimal
            }
        }
    },

    "IRELAND_2023":{
        "grain":["beneficiary", "intervention_code", "municipality"],

        "preprocessor": no_preprocessing,
        "columns": {
            "beneficiary":{
                "source":"name_of_the_beneficiary_legal_entity_association",
                "transform": None
            },
            "municipality":{
                "source":"municipality",
                "transform": None
            },
            "country":{
                "source": "source_country",
                "transform": None
            },
            "year":{
                "source": "source_year",
                "transform": None
            },
            "intervention_code":{
                "source": "code_of_measure_type_of_intervention_sector_as_set_in_annex_ix",
                "transform": None
            },
            "total_eagf_income_support":{
                "source": "amount_by_operation_under_eagf",
                "transform": cast_only
            },
            "total_eafrd_income_support":{
                "source": "amount_by_operation_under_eafrd",
                "transform": cast_only
            },
            "national_cofunding_amount":{
                "source": "amount_by_operation_under_co_financing",
                "transform": cast_only
            }
        }
    },
        
    "IRELAND_2024":{
        "grain":["beneficiary", "intervention_code", "municipality"],

        "preprocessor": second_row_header,
        "columns":{
            "beneficiary":{
                "source":"Name of the beneficiary/Legal entity/association",
                "transform": None
            },
            "municipality":{
                "source":"Municipality",
                "transform": None
            },
            "country":{
                "source": "source_country",
                "transform": None
            },
            "year":{
                "source": "source_year",
                "transform": None
            },
            "intervention_code":{
                "source": "Code of measure type of intervention/sector as set in Annex IX",
                "transform": None
            },
            "total_eagf_income_support":{
                "source": "Amount by operation under EAGF",
                "transform": cast_only
            },
            "total_eafrd_income_support":{
                 "source": "Amount by operation under EAFRD",
                "transform": cast_only
            },
            "national_cofunding_amount":{
                "source": "Amount by operation under co-financing",
                "transform": cast_only
            }
        }
    },

    "GERMANY_2023": {
        "grain":["beneficiary", "intervention_code", "municipality"],

        "preprocessor": no_preprocessing,
        "columns": {
            "beneficiary":{
                "source":"name_des_begã¼nstigten_rechtstrã¤gers_verdands",
                "transform": None
            },
            "municipality":{
                "source":"gemeinde",
                "transform": None
            },
            "country":{
                "source": "source_country",
                "transform": None
            },
            "year":{
                "source": "source_year",
                "transform": None
            },
            "intervention_code":{
                "source": "code_der_maãnahme_der_interventionskategorie_des_sektors_gemã¤ã_anhang_ix",
                "transform": None
            },
            "total_eagf_income_support":{
                "source": "betrag_je_vorhaben_im_rahmen_des_egfl",
                "transform": cast_only
            },
            "total_eafrd_income_support":{
                 "source": "betrag_je_vorhaben_im_rahmen_des_eler_(eu_mittel)",
                "transform": cast_only
            },
            "national_cofunding_amount":{
                "source": "betrag_je_vorhaben_im_rahmen_der_nationalen_kofinanzierung",
                "transform": cast_only
            }
        }
    },
    "GERMANY_2024": {
        "grain":["beneficiary", "intervention_code", "municipality"],

        "preprocessor": no_preprocessing,
        "columns": {
            "beneficiary":{
                "source":"name_des_begã¼nstigten_rechtstrã¤gers_verdands",
                "transform": None
            },
            "municipality":{
                "source":"gemeinde",
                "transform": None
            },
            "country":{
                "source": "source_country",
                "transform": None
            },
            "year":{
                "source": "source_year",
                "transform": None
            },
            "intervention_code":{
                "source": "code_der_maãnahme_der_interventionskategorie_des_sektors_gemã¤ã_anhang_ix",
                "transform": None
            },
            "total_eagf_income_support":{
                "source": "betrag_je_vorhaben_im_rahmen_des_egfl",
                "transform": cast_only
            },
            "total_eafrd_income_support":{
                "source": "betrag_je_vorhaben_im_rahmen_des_eler_(eu_mittel)",
                "transform": cast_only
            },
            "national_cofunding_amount":{
                "source": "betrag_je_vorhaben_im_rahmen_der_nationalen_kofinanzierung",
                "transform": cast_only
            }
        }
    }
}


