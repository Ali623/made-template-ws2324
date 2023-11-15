import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import ETL

# main function
def main():
    file1 = "https://drive.google.com/uc?export=download&id=1bOcgQ9hBH5F_jLwbrcTJY4JLLn6_Z_EH"
    file2 = "https://drive.google.com/uc?export=download&id=1eLinVTl2DyUst8yJ4DCbBhoS-swW930W"

    df1 = ETL.Extract(file1)
    column_map1 = {
        "PFLEGEOBJE" : "Care_Object",
        "Objekttyp" : "Object_Type",
        "Bezirk" : "District",
        "X_Koordina" : "X_coordinate",
        "Y_Koordina" : "Y_coordinate",
        "STAMMVON" : "Tribe_of",
        "STAMMBIS" : "Stem_bis",
        "KRONE" : "Crown",
        "HöHE" : "Height",
        "AlterSchätzung" : "Age_estimate"
    }

    df1 = ETL.Transform(df1, column_map1)
    ETL.Load(df1, "table_1")

    df2 = ETL.Extract(file2)
    column_map2 = {
        "BAUMNUMMER" : "Tree_number",
        "HOCHWERT" : "High_value",
        "RECHTSWERT" : "Legal_value",
        "GATTUNGART" : "Genus_type",
        "GATTUNG" : "Genus",
        "GA_LANG" : "GA_LANG",
        "KR_DURCHM" : "Kr_diam",
        "ST_UMFANG" : "ST_Scope",
        "GEBIET" : "Area",
        "STRASSE" : "Street",
        "BAUMHOEHE" : "Tree_Height",
        "ST_DURCHM" : "St_diam",
        "PFLANZJAHR" : "Planting_year",
        "Kr_Radius" : "Kr_Radius",
    }

    df2 = ETL.Transform(df2, column_map2)
    ETL.Load(df2, "table_2")

if __name__ == "__main__":
    main()