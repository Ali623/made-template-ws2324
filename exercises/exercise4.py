import os
import sys
import zipfile
import pandas as pd
from urllib.request import urlretrieve
from sqlalchemy import create_engine, text

def extract_data(download_url, zip_file_path, data_folder):
    # Download the ZIP file
    urlretrieve(download_url, zip_file_path)

    # Unzip the file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(data_folder)


def reshape_data(data_csv_path):
    # Read the CSV file
    df = pd.read_csv(data_csv_path, sep=";", decimal=",", index_col=False,
                            usecols=["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
                                     "Batterietemperatur in °C", "Geraet aktiv"])

    # Rename columns
    df = df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"})

    return df

def transform_data(df):
    # Transform temperatures to Fahrenheit
    df["Temperatur"] = (df["Temperatur"] * 9/5) + 32
    df["Batterietemperatur"] = (df["Batterietemperatur"] * 9/5) + 32

    return df

def validate_data(df):
    # For simplicity, let's assume 'Geraet' should be a positive integer
    df = df[df['Geraet'] > 0]
    df = df[df['Hersteller'].astype(str).str.strip().ne("")]
    df = df[df['Model'].astype(str).str.strip().ne("")]
    df = df[df['Monat'].between(1, 12)]
    df = df[pd.to_numeric(df['Temperatur'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['Batterietemperatur'], errors='coerce').notnull()]
    df = df[df['Geraet aktiv'].isin(['Ja', 'Nein'])]

    return df



def save_to_db(data_frame, database_name, table_name):
    # Create SQLite engine
    engine = create_engine(f'sqlite:///{database_name}')

    # Create or replace table
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Geraet INTEGER,
            Hersteller TEXT,
            Model TEXT,
            Monat TEXT,
            Temperatur REAL,
            Batterietemperatur REAL,
            Geraet_aktiv TEXT
        )
    """
    with engine.connect() as connection:
        connection.execute(text(create_table_query))

    # Write DataFrame to SQLite
    data_frame.to_sql(table_name, engine, if_exists='replace', index=False)


def main():
    # Step 1: Extraction
    download_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
    zip_file_path = "mowesta-dataset.zip"
    data_folder = "mowesta-dataset"

    extract_data(download_url, zip_file_path, data_folder)

    # Step 2: Reshape
    data_csv_path = os.path.join(data_folder, "data.csv")

    df = reshape_data(data_csv_path)
    print(df.head())

    # Step 3: Transform
    df = transform_data(df)
    print(df.head())

    # Step 4: Validate
    df = validate_data(df)
    print(df.head())

    # Step 5: Save to database
    db_path = "temperatures.sqlite"
    table_name = "temperatures"

    save_to_db(df, db_path, table_name)

    print("Data pipeline executed successfully.")

if __name__ == "__main__":
    main()
