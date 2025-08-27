import sys
import time
#import argparse to allow command line arguments
import argparse

#import helper functions from edge_card.py
from edge.edge_card import wait_for_card, pack_tx, read_recent_tx, write_recent_tx
#import rules wrapper
from edge.edge_rules import EdgeRules

#constant for merchant ID, used in transactions
MERCHANT_ID = 1234

#instantiate the edge rules object (add path to zipcode CSV file)
rules = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")

#fucntion to process a transaction
#waits for a card, reads recent transactions, evaluates rules, and appends the new transaction
#takes command line arguments for amount, merchant ID, and zip code
#returns nothing, prints results to console
def process_transaction(args):
    print("\nTap card to process transaction …")
    #wait for a card to be tapped
    uid = wait_for_card(timeout=60)
    #if no card is detected, print a message and return
    if not uid:
        print("No card detected.")
        return
    #set card ID from the UID
    card_id = uid.hex()
    #read recent transactions from the card up to 10 txs
    meta, recent = read_recent_tx(uid, max_count=10)

    #warmup the rules using on-card history
    rules.warmup_from_card(card_id, list(reversed(recent)))

    #create transaction dictionary for rules evaluation
    tx = {
        "timestamp": int(time.time()),
        "merchant_id": args.merchant,
        "card_id": card_id,
        "amount": args.amount_cents / 100.0,
        "zip": args.zip, 
        "lat": None,
        "lon": None,
    }

    #debugging output (show transaction coords were found from zip code)
    #print("ZIP→coords:", rules.travel.lookup.get(tx["zip"]) if tx["zip"] else None)

    #evaluate the transaction against the edge rules
    edge_flag, reasons = rules.evaluate(tx)
    #print the evaluation result
    print("EDGE CHECK:", "FLAGGED" if edge_flag else "OK", reasons)

    #pack the transaction record to append to the card
    rec = pack_tx(
        timestamp=tx["timestamp"],
        amount_cents=args.amount_cents,
        merchant_id=args.merchant, 
        zip_code=args.zip, 
        flags=1 if edge_flag else 0
    )
    ok, msg = write_recent_tx(uid, rec)
    print("Append:", ok, msg)

#main function to run the script
if __name__ == "__main__":
    #set up argument parser
    p = argparse.ArgumentParser()
    #add arguments for amount, merchant ID, and zip code
    p.add_argument("amount_cents", type=int)
    p.add_argument("--merchant", type=int, default=MERCHANT_ID)
    p.add_argument("--zip")
    #parse the command line arguments
    args = p.parse_args()

    #process the transaction with the arguments
    process_transaction(args)