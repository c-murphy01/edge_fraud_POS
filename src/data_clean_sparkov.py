#import libraries
import pandas as pd
from pathlib import Path

#define constant paths for input and output
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR/"data"/"raw"/"sparkov"
PRO_DIR = BASE_DIR/"data"/"processed"
OUT_PATH = PRO_DIR/"sparkov_pos_sorted.csv"

#constant for the columns we want to keep
COLUMNS = ["trans_date_trans_time", "cc_num", "merchant",
                 "category", "amt", "city", "state", "zip", "is_fraud"]

#define function to load the two data files and concatenate them
def load_concat_data(train_path, test_path):
    #for each file only copy the defined columns, and pasre the transdate+time column in date format
    train = pd.read_csv(train_path, usecols=COLUMNS, parse_dates=["trans_date_trans_time"])
    test = pd.read_csv(test_path, usecols=COLUMNS, parse_dates=["trans_date_trans_time"])

    return pd.concat([train, test], ignore_index=True)

#function to clean the data
def clean(df):
    #this is the rule we gotr from the exploratory analysis
    #we copy all of the data except for rows where the category column ends in '_net'
    df = df[~df["category"].str.endswith("_net")].copy()
    #rename several columns to match the columns in the caixa dataset
    df.rename(columns={
        "trans_date_trans_time": "timestamp",
        "cc_num": "card_id",
        "merchant": "merchant_id",
        "amt": "amount",
        "city": "merchant_city",
        "state": "merchant_state",
    }, inplace=True)

    #cast ids to strings for easier logic later
    df["merchant_id"] = df["merchant_id"].astype(str)
    df["card_id"]     = df["card_id"].astype(str)

    # sort by time (and merchant to be deterministic within ties)
    df.sort_values(["timestamp", "merchant_id"], inplace=True)

    #return the new, cleaned dataframe
    return df[["timestamp", "card_id", "merchant_id", "amount", "category",
                "merchant_city", "merchant_state", "zip", "is_fraud"]]

def process_sparkov(out_path = OUT_PATH):
    #function calls to load, concat, and clean the sparkov data
    df_raw = load_concat_data(RAW_DIR/"fraudTrain.csv", RAW_DIR/"fraudTest.csv")
    df_pos = clean(df_raw)
    if out_path is not None: 
        #ensure the output folder exists
        PRO_DIR.mkdir(parents=True, exist_ok=True)
        #write the data to disk memory, removing the pandas indexing
        df_pos.to_csv(out_path, index=False)
    return df_pos

if __name__ == "__main__":
    df = process_sparkov(OUT_PATH)
    #print a statement to confirm it worked
    print(f"wrote {len(df):,} POS rows to {OUT_PATH}")