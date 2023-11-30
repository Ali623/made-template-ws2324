# import necessary libraries and file
import pandas as pd
from sqlalchemy import create_engine, inspect
from pipeline import Extract, Transform

# define function to test the extraction process

def perform_extraction_test(file_path):
    extracted_data = Extract(file_path)

    if extracted_data.empty:
        raise AssertionError("Extraction failed")
    
    print("perform_extraction_test: Test Passed")
    return extracted_data


# define function to test the transformation process

def perform_transformation_test(data, column_map, col_to_drop):
    transformed_df = Transform(data, column_map, col_to_drop)
    
    if transformed_df.isna().any().any():
        raise AssertionError("NAN Found in Data")
    
    print("perform_transformation_test: Test Passed")
    return transformed_df


# define function to test the loading process

def perform_data_loading_test(table_name):
    engine = create_engine(f"sqlite:///../data/Tree.sqlite")

    # Create an inspector object
    inspector = inspect(engine)

    # Check if a table exists in the database
    if not inspector.has_table(table_name):
        raise AssertionError(f"The table '{table_name}' does not exist in the database.")
    
    print(f"perform_data_loading_test: Table '{table_name}' exists, Test Passed")

# define main function

def main():
    file1 = "https://offenedaten-koeln.de/sites/default/files/Bestand_Einzelbaeume_Koeln_0.csv"
    file2 = "https://offenedaten.frankfurt.de/dataset/73c5a6b3-c033-4dad-bb7d-8783427dd233/resource/e53aacb4-4462-4b69-ab9f-4252a402a082/download/baumauswahl_veroffentlichung_8-berbeitetrkr.csv"

    # Execution block for dataset 1
    df1 = perform_extraction_test(file1)
    col_to_drop1 = ["Baum-Nr.","HausNr","Lage","Gattung","Art","Sorte","DeutscherN"]
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

    df1 = perform_transformation_test(df1, column_map1, col_to_drop1)
    perform_data_loading_test("table_1")

    # Execution block for dataset 2
    df2 = perform_extraction_test(file2)
    col_to_drop2 = ["STANDORT","BAUM_STATU","GATTUNG","GATTUNGART","GA_LANG","GEBIET","STRASSE"]
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

    df2 = perform_transformation_test(df2, column_map2, col_to_drop2)
    perform_data_loading_test("table_2")

# execute the main function
if __name__ == "__main__":
    main()