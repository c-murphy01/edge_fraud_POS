import pandas as pd
import numpy as np

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
        #create a new random card number with prefix SYN to show that the card is synthetic
        new_card_id = f"SYN{rng.integers(10**10, 10**11)}"

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