#import libraries
import pandas as pd
from pathlib import Path

#define constant paths for input and output
RAW_DIR = Path("../data/raw")
OUT_PATH = Path("../data/processed/sparkov_pos_sorted.csv")

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

    #return the new, cleaned dataframe
    return df[["timestamp", "card_id", "merchant_id", "amount", "category",
                "merchant_city", "merchant_state", "zip", "is_fraud"]]


if __name__ == "__main__":
    #function calls to load, concat, and clean the sparkov data
    df_raw = load_concat_data(RAW_DIR/"fraudTrain.csv",
                         RAW_DIR/"fraudTest.csv")
    df_pos = clean(df_raw)

    #write the data to disk memory, removing the pandas indexing
    df_pos.to_csv(OUT_PATH, index=False)
    #print a statement to confirm it worked
    print(f"wrote {len(df_pos):,} POS rows to {OUT_PATH}")