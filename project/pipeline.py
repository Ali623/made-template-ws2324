import pandas as pd
from sqlalchemy import create_engine

# To extract the data from csv file
def Extract(file):
    df = pd.read_csv(file, delimiter=";")
    return df

# To transform the data (change column labels)
def Transform(df, column_map, col_to_drop):
    df = df.drop(columns = col_to_drop)
    df = df.rename(columns=column_map)
    return df

# Load data to create a SQLite file
def Load(df, table):
    engine = create_engine(f"sqlite:///../data/Tree.sqlite")
    df.to_sql(table, engine, if_exists="replace")

# main function
def main():
    file1 = "https://offenedaten-koeln.de/sites/default/files/Bestand_Einzelbaeume_Koeln_0.csv"
    file2 = "https://offenedaten.frankfurt.de/dataset/73c5a6b3-c033-4dad-bb7d-8783427dd233/resource/e53aacb4-4462-4b69-ab9f-4252a402a082/download/baumauswahl_veroffentlichung_8-berbeitetrkr.csv"

    df1 = Extract(file1)
    col_to_drop1 = ["Baum-Nr.","HausNr","Lage","Art","Sorte","DeutscherN"]
    column_map1 = {
        "PFLEGEOBJE" : "Care_Object",
        "Objekttyp" : "Object_Type",
        "Bezirk" : "District",
        "X_Koordina" : "x_coordinate",
        "Y_Koordina" : "y_coordinate",
        "STAMMVON" : "Tribe_of",
        "STAMMBIS" : "trunk diameter",
        "KRONE" : "Crown",
        "HöHE" : "Height",
        "AlterSchätzung" : "Age",
        "Gattung" : "Genus"
    }

    df1 = Transform(df1, column_map1, col_to_drop1)
    Load(df1, "table_1")

    df2 = Extract(file2)
    col_to_drop2 = ["STANDORT","BAUM_STATU","GATTUNGART","GA_LANG","GEBIET","STRASSE"]
    column_map2 = {
        "BAUMNUMMER" : "Tree_number",
        "HOCHWERT" : "y_coordinate",
        "RECHTSWERT" : "x_coordinate",
        "GATTUNGART" : "Genus_type",
        "GATTUNG" : "Genus",
        "GA_LANG" : "GA_LANG",
        "KR_DURCHM" : "trunk diameter",
        "ST_UMFANG" : "ST_Scope",
        "GEBIET" : "Area",
        "STRASSE" : "Street",
        "BAUMHOEHE" : "Height",
        "ST_DURCHM" : "St_diam",
        "PFLANZJAHR" : "Planting_year",
        "Kr_Radius" : "trunk radius",
    }

    df2 = Transform(df2, column_map2, col_to_drop2)
    Load(df2, "table_2")

if __name__ == "__main__":
    main()