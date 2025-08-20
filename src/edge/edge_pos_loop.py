import sys
import time
#import helper functions from edge_card.py
from edge.edge_card import wait_for_card, pack_tx, read_recent_tx, write_recent_tx
#import rules wrapper
from edge.edge_rules import EdgeRules

#constant for merchant ID, used in transactions
MERCHANT_ID = 1234

#instantiate the edge rules object (add path to zipcode CSV file)
rules = EdgeRules(zip_csv_path="data/zip_lat_long.csv")

#fucntion to process a transaction
#waits for a card, reads recent transactions, evaluates rules, and appends the new transaction
#amount_cents is the transaction amount in cents
#returns nothing, prints results to console
def process_transaction(amount_cents: int):
    print("Tap card to process transaction …")
    #wait for a card to be tapped
    uid = wait_for_card(timeout=60)
    #if no card is detected, print a message and return
    if not uid:
        print("No card detected.")
        return
    #set card ID from the UID
    card_id = uid.hex()
    #read recent transactions from the card up to 30 txs
    meta, recent = read_recent_tx(uid, max_count=30)

    #warmup the rules using on-card history
    rules.warmup_from_card(card_id, list(reversed(recent)))

    #create transaction dictionary for rules evaluation
    tx = {
        "timestamp": int(time.time()),
        "merchant_id": MERCHANT_ID,
        "card_id": card_id,
        "amount": amount_cents / 100.0,
        "zip": None, 
        "lat": None,
        "long": None,
    }

    #evaluate the transaction against the edge rules
    edge_flag, reasons = rules.evaluate(tx)
    #print the evaluation result
    print("EDGE CHECK:", "FLAGGED" if edge_flag else "OK", reasons)

    #pack the transaction record to append to the card
    rec = pack_tx(
        timestamp=tx["timestamp"],
        amount_cents=amount_cents,
        merchant_id=MERCHANT_ID,
        flags=1 if edge_flag else 0
    )
    ok, msg = write_recent_tx(uid, rec)
    print("Append:", ok, msg)

#main function to run the script
if __name__ == "__main__":
    #if an amount is provided as a command line argument, use it, otherwise default to 999 cents
    amount_cents = int(sys.argv[1]) if len(sys.argv) > 1 else 999
    process_transaction(amount_cents)