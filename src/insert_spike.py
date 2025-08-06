import pandas as pd
import numpy as np
from pathlib import Path

#function to add a spike of card transactions at a random time window for one merchant
def add_merchant_spike(df, row, cards, window):
    #create an instnace of a random number generator
    rng = np.random.default_rng()

    #get the window start time from the inputted row's timestamp
    start_time = row.timestamp.floor(f"{window}s")

    #empty array to store new rows
    new_rows = []

    #for loop to loop through each card
    for _ in range(cards):
        #create a new random card number with prefix MSPIKE to show that the card is synthetic
        new_card_id = f"MSPIKE{rng.integers(10**10, 10**11)}"

        #offset the time by a random amount
        offset = int(rng.integers(0, window))
        new_time = start_time + pd.Timedelta(seconds=offset)

        #append new rows to the array, each has the same features as a normal transaction
        new_rows.append(
            {
                "timestamp": new_time,
                "card_id": new_card_id,
                "merchant_id": row.merchant_id,
                "merchant_city": row.merchant_city,
                "category": row.category,
                "amount": float(row.amount),
                "is_fraud": 1
            }
        )

    #output the dataframe with teh new rows added to it
    df_output = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return df_output

#fucntion to create a spike of uses of a single card at multiple merchants
def add_card_burst(df,card_id, start_time, merchant_list, tx_per_merch, spacing):
    
    #empoty array to store rows from df
    rows = []
    #set current time to start time argument
    current_t = start_time

    #fro each merchant in the list
    for merch_id in merchant_list:
        #get a reference row for this merchant id
        #if this merchant is not in the dataframe somehow, continiue to next merchant
        if df[df["merchant_id"] == merch_id].empty:
            continue
        #row_ref holds the first row in a new df containing only rows where the merchant_id matches merch_id
        row_ref = df[df["merchant_id"] == merch_id].iloc[0]

        #for loop to add synthetic transactions
        for _ in range(tx_per_merch):
            #append new transactions to rows array, taking merchant attributes from reference
            rows.append(
                {
                    "timestamp": current_t,
                    "card_id": card_id,
                    "merchant_id": merch_id,
                    "merchant_city": row_ref.merchant_city,
                    "category": row_ref.category,
                    "amount": float(row_ref.amount),
                    "is_fraud": 1,
                }
            )

            #adjust current time by spacing amount
            current_t += pd.Timedelta(seconds=spacing)

    #return the dataframe with synthetic transactions added
    return pd.concat([df, pd.DataFrame(rows)], ignore_index=True)


if __name__ == "__main__":
    #define constants for filepaths
    PATH = Path("data/processed")
    INPUT = PATH / "sparkov_pos_sorted.csv"
    OUTPUT = PATH / "sparkov_spikes.csv"
    TRUTH_MERCH = PATH / "synth_truth_merchant.csv"
    TRUTH_CARD = PATH / "synth_truth_card.csv"

    #set seed for random number generator to make spikes reproducable
    rng = np.random.default_rng(13)

    #read sorted data to dataframe and print rows before injection, parses the timestamps to pandas
    df = pd.read_csv(INPUT, parse_dates=["timestamp"])
    print("Rows befroe injection: ", len(df))

    #empty lsit to store ground truth windows
    merch_windows = []

    #smaple 50 random merchants (seeded randomness)
    samples = df.sample(50, random_state=17)
    #for each tuple in list of samples
    for tpl in samples.itertuples():
        #call the function to add a spike of 15 cards into 30s window
        df = add_merchant_spike(df, tpl, 15, 30)
        #find 30s bucket that spike will be injected into
        bucket = tpl.timestamp.floor("30s")
        #record the spikes location to measure recall later
        merch_windows.append({"merchant_id": tpl.merchant_id, "bucket_30s": bucket})

    #empty list to store ground truths for card bursts
    card_windows =[]

    #create array of all unique mechants
    merchant_pool = df["merchant_id"].unique()
    #loop for 50 different card bursts
    for i in range(50):
        #create new synthetic card number with prefix CBURST
        base_card  = f"CBURST{rng.integers(10**10, 10**11)}"
        #choose 4 random distinct merchants
        merch_list     = rng.choice(merchant_pool, size=4, replace=False)
        #choose random start time sometime in the two years the original data spans
        start_ts   = pd.Timestamp("2019-01-01") + pd.Timedelta(
            seconds=int(rng.integers(0, 60*60*24*730)))
        #inject the card burst
        #the card taps two times at each of the 4 merchants with 30s gap between taps
        df = add_card_burst(df, base_card, start_ts, merch_list, 2, 30)
        #save the ground truth info
        card_windows.append({"card_id": base_card, "burst_start": start_ts})
    
    #resort the data by timestamp and write the new spiked data to a csv file
    df.sort_values(["timestamp", "merchant_id"], inplace=True)
    df.to_csv(OUTPUT, index=False)
    #save the two ground truth files to csv
    pd.DataFrame(merch_windows).to_csv(TRUTH_MERCH, index=False)
    pd.DataFrame(card_windows).to_csv(TRUTH_CARD, index=False)

    #print various checks to make sure the script worked
    print("Rows after injection: ", len(df))
    print("merchant bursts saved to", TRUTH_MERCH.name)
    print("card bursts saved to   ", TRUTH_CARD.name)
    print("augmented file saved to", OUTPUT.name)