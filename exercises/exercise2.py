import pandas as pd
from sqlalchemy import create_engine

# To extract the data from csv file
def Extract(file):

    df = pd.read_csv(file, sep=';', low_memory=False)
    return df

# To transform the data (change column labels)
def Transform(data):
    
    # 1.Dropping the "Status" column from the extracted dataframe
    data = data.drop('Status', axis=1)

    # Replacing "," between the values by "."
    data['Laenge'] = data['Laenge'].str.replace(',', '.').astype(float)
    data['Breite'] = data['Breite'].str.replace(',', '.').astype(float)

    # 2.Validating the data according to the given conditions
    # Valid "Verkehr" values are "FV", "RV", "nur DPN"
    # Valid "Laenge" values are geographic coordinate system values between and including -90 and 90
    # Valid "Breite" values are geographic coordinate system values between and including -90 and 90
    # Valid "IFOPT" values are
    #   1. <exactly two characters>:<any amount of numbers>:<any amount of numbers><optionally another colon followed by any amount of numbers>
    # Drop rows with empty cells

    data = data[
        (data["Verkehr"].isin(["FV", "RV", "nur DPN"])) &
        (data["Laenge"].between(-90, 90)) &
        (data["Breite"].between(-90, 90)) &
        (data["IFOPT"].str.match(r"^[A-Za-z]{2}:\d+:\d+(?::\d+)?$"))
        ].dropna()

    # 3.Type casting the data accordingly
    type_casting = {
        "EVA_NR": int,
        "DS100": str,
        "IFOPT": str,
        "NAME": str,
        "Verkehr": str,
        "Laenge": float,
        "Breite": float,
        "Betreiber_Name": str,
        "Betreiber_Nr": int
    }
    data = data.astype(type_casting)
    
    return data

# Load data to create a SQLite file
def Load(data, table_name):
        
    engine = create_engine("sqlite:///trainstops.sqlite")
    data.to_sql(table_name, engine, if_exists="replace", index=False)


# main function
def main():
    data_path = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    data = Extract(data_path)
    if data is not None:
        data = Transform(data)
        if data is not None:
            Load(data, "trainstops")
        else:
            print("Data transformation is failed")
    else:
        print("Data extraction is failed")


if __name__ == "__main__":
    main()