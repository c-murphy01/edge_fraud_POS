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

    #retrun the new rows as dataframe
    return pd.DataFrame(new_rows)

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

    #return the rows as dataframe
    return pd.DataFrame(rows)

#fucntion to insert merchant spikes and card bursts into dataframe and return the augmented df and truth tables
def insert_spikes_to_data(df, n_merch_spikes, n_card_bursts, cards_per_spike, merchant_window,
                  tx_per_merch, spacing, seed_spikes, seed_samples):
    #create copy of df
    df = df.copy()
    #ensure meach and card ids are strings
    df["merchant_id"] = df["merchant_id"].astype(str)
    df["card_id"] = df["card_id"].astype(str)

    #create seeded random number generator
    rng_spike = np.random.default_rng(seed_spikes)

    #sample n merchants
    samples = df.sample(n_merch_spikes, random_state=seed_samples)
    #create empty lists to store now rows and truth rows in
    merch_truth_rows = []
    merch_new_rows = []

    #for each sampled row
    for tpl in samples.itertuples():
        #add a merchant spike
        spike_df = add_merchant_spike(df, tpl, cards_per_spike, merchant_window)
        #add spike to new_rows
        merch_new_rows.append(spike_df)
        #calc bucket that spike starts in
        bucket = tpl.timestamp.floor("30s")
        #add merch_id and spike timestamp (buccket) to truth rows 
        merch_truth_rows.append({"merchant_id": str(tpl.merchant_id), "bucket_30s": bucket})

    #create empty lists for new card rows and ttruth
    card_truth_rows = []
    card_new_rows = []

    #create array of all unique mechants
    merchant_pool = df["merchant_id"].unique()
    #find range of timestamps from data
    tmin, tmax = df["timestamp"].min(), df["timestamp"].max()
    total_seconds = int((tmax - tmin).total_seconds())

    #for each card burst
    for _ in range(n_card_bursts):
        #generate random card number with prefix "CBURST"
        base_card  = f"CBURST{rng_spike.integers(10**10, 10**11)}"
        #choose 4 random distinct merchants
        merch_list = rng_spike.choice(merchant_pool, size=4, replace=False)
        #choose random start time sometime span of the data
        start_ts = tmin + pd.Timedelta(seconds=int(rng_spike.integers(0, max(total_seconds, 1))))
        #add a card burst
        burst_df = add_card_burst(df, base_card, start_ts, merch_list, tx_per_merch, spacing)
        #add new rows to list
        card_new_rows.append(burst_df)
        #add card id and start time to truth table
        card_truth_rows.append({"card_id": base_card, "burst_start": start_ts})

    #create new augmented df with spikes and bursts added
    df_aug = pd.concat([df] + merch_new_rows + card_new_rows, ignore_index=True)
    #sort by timestamp and merchant id
    df_aug.sort_values(["timestamp", "merchant_id"], inplace=True)

    #create dataframes for truths
    truth_m = pd.DataFrame(merch_truth_rows)
    truth_c = pd.DataFrame(card_truth_rows)

    #return the new dataframes
    return df_aug, truth_m, truth_c

if __name__ == "__main__":
    #define constants for filepaths
    PATH = Path("data/processed")
    INPUT = PATH / "sparkov_pos_sorted.csv"
    OUTPUT = PATH / "sparkov_spikes.csv"
    TRUTH_MERCH = PATH / "synth_truth_merchant.csv"
    TRUTH_CARD = PATH / "synth_truth_card.csv"

    #read sorted data to dataframe and print rows before injection, parses the timestamps to pandas
    df0 = pd.read_csv(INPUT, parse_dates=["timestamp"])
    print("Rows before injection:", len(df0))

    #run function to spike data
    df_out, tm, tc = insert_spikes_to_data(df0)

    #write outoputs to output files
    df_out.to_csv(OUTPUT, index=False)
    tm.to_csv(TRUTH_MERCH, index=False)
    tc.to_csv(TRUTH_CARD, index=False)

    #print various checks to make sure the script worked
    print("Rows after injection: ", len(df_out))
    print("merchant bursts saved to", TRUTH_MERCH.name)
    print("card bursts saved to   ", TRUTH_CARD.name)
    print("augmented file saved to", OUTPUT.name)