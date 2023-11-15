import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# To extract the data from csv file
def Extract(file):
    df = pd.read_csv(file, delimiter=",")
    return df

# To transform the data (change column labels)
def Transform(df, column_map):
    df = df.rename(columns=column_map)
    return df

# Load data to create a SQLite file
def Load(df, table):
    engine = create_engine(f"sqlite:///data/Tree.sqlite")
    df.to_sql(table, engine, if_exists="replace")