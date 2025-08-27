#import libraries and functions
import csv, time, os, datetime, random
from edge.edge_rules import EdgeRules
from edge.edge_card  import wait_for_card, read_recent_tx, pack_tx, write_recent_tx

#set path to log Tx records
LOG_PATH = "data/runs/pos_log.csv"

#function to write row to log
def log_row(tx, edge_flag, reasons):
    #make sure directory exists
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    exists = os.path.exists(LOG_PATH)
    #open the file
    with open(LOG_PATH, "a", newline="") as f:
        #create csv writer object
        w = csv.writer(f)
        #if the file doesn't exist, write the column names / metadata
        if not exists:
            w.writerow(["ts_iso","uid","amount_cents","merchant_id","flag","reasons"])
        #write the transaction details to log
        w.writerow([
            datetime.datetime.fromtimestamp(tx["timestamp"]).isoformat(timespec="seconds"),
            tx["card_id"],
            tx["amount_cents"],
            tx["merchant_id"],
            int(edge_flag),
            "|".join(reasons),
        ])

#function to wait for next 30s window to start
#used to ensure that the demo does not run too fast and trigger the same rules repeatedly
def wait_for_window():
    #get current time
    now = time.time()
    #find remaining seconds until next 30s window
    remaining = 31 - (int(now) % 30)
    #print message and sleep until next window
    print(f"\n[demo] waiting {remaining}s to clear 30s window…")
    time.sleep(remaining)

#function to evaluater
def eval_and_write(uid, rules, merchant_id, amount_cents=None, zipc=None, lat=None, lon=None, note=""):
    #generate random transaction value from €1 - €40
    if amount_cents is None:
        amount_cents = random.randint(100, 4000)
    #warmup already done by caller
    tx = {
        "timestamp": int(time.time()),
        "merchant_id": merchant_id,
        "card_id": uid.hex(),
        "amount": amount_cents / 100.0,
        "zip": zipc, "lat": lat, "lon": lon,
        "amount_cents": amount_cents,
    }
    #evaluate transaction
    flag, reasons = rules.evaluate(tx)
    ts_readable = datetime.datetime.fromtimestamp(tx["timestamp"]).strftime("%H:%M:%S")
    #print tx details
    print(f"TX details[ time: {ts_readable}; merchant ID: {merchant_id}; "
        f"Card ID: {uid.hex()}; amount: €{tx['amount']:.2f}; location: {zipc} ]")
    #print the eval results
    print(f"{note} EDGE CHECK:", "FLAGGED" if flag else "OK", reasons)
    #pack the transaction details
    rec = pack_tx(tx["timestamp"], amount_cents, merchant_id, 1 if flag else 0)
    #write the detasils to the card
    ok, msg = write_recent_tx(uid, rec)
    #show that the tx was recorded
    print("Append:", ok, msg)
    #log the transaction
    log_row(tx, flag, reasons)
    return flag, reasons

def demo_walkthrough():
    #function to instantiate a new rules object
    def new_ruleset(uid=None):
        r = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")
        if uid:
            #get metadata and recent Tx
            meta, recent = read_recent_tx(uid, max_count=10)
            #initialise rules from recent tx
            r.warmup_from_card(uid.hex(), list(reversed(recent)))
            #return warmed up ruleset
        return r

    #demonstrate the edge rules in action
    #Test 1: card baseline (>= 3 different merchants in 30s with one card)
    print("\n1. Card Threshold Test.")
    print("Three transactions at different merchants.\n")
    print("Please tap and hold a card on the reader!")
    time.sleep(3)
    uid = wait_for_card(timeout=60)
    rules = new_ruleset(uid)
    #create transactions with 3 different merchants in 30s
    eval_and_write(uid, rules, merchant_id=1001, note="\n[CardBaseline 1]")
    eval_and_write(uid, rules, merchant_id=1002, note="\n[CardBaseline 2]")
    eval_and_write(uid, rules, merchant_id=1003, note="\n[CardBaseline 3]")
    
    #wait for the next 30w window so flags dont carry over
    wait_for_window()

    #Test 2: merchant baseline
    #3 different cards transacting with the same merchant in 30 seconds
    #adjust merchant threshold to 3 just for this test (easier to demo)
    rules.merchant.th = 3

    print("\n2. Merchant Threshold Test.")
    print("One transaction with three different cards at the same merchant.")
    print("Note: The normal threshold of 6 is changed to 3 for demonstration puposes.\n")
    #transaction for card A
    print("Tap CARD A!")
    time.sleep(3)
    uidA = wait_for_card(timeout=60)
    eval_and_write(uidA, rules, merchant_id=2001, note="\n[Merchant A]")
    print("Card ID: ", uidA.hex(), "\n")
    #sleep for a couple of seconds so user can change card
    time.sleep(2)

    #transaction for card B
    print("Now tap CARD B!")
    time.sleep(3)
    uidB = wait_for_card(timeout=60)
    eval_and_write(uidB, rules, merchant_id=2001, note="[Merchant B]")
    print("Card ID: ", uidB.hex(), "\n")
    time.sleep(2)

    #transaction for card C
    print("Finally tap CARD C!")
    time.sleep(3)
    uidC = wait_for_card(timeout=60)
    eval_and_write(uidC, rules, merchant_id=2001, note="[Merchant C]")
    print("Card ID: ", uidC.hex())
    time.sleep(5)

    #wait for new window again
    wait_for_window()

    #Test 3: card EWMA: 6 Tx, small, 'normal' values for first 5, then unusually large value for Tx 6
    print("\n3. Card EWMA Test.")
    print("Six transactions created: First 5 are 'normal' transaction values, 6th is 'unusaully large for this card.")
    print("Note: The normal initial 'warm up' transactions required of 10 is changed to 5 for demonstration puposes.\n")

    print("Please tap and hold a card on the reader!")
    time.sleep(3)
    uid = wait_for_card(timeout=60)
    #reinstantiate and warm up rule objects to avoid extra flags in demo 
    rules = new_ruleset(uid)
    #change initial tx needed for warm up to 5 for demo
    rules.ewma.initial = 5
    #create 5 transactions with 'normal amounts' for thsi card so thast there is a baseline for EWMA
    eval_and_write(uid, rules, merchant_id=3001, note="\n[EWMA baseline 1]")
    eval_and_write(uid, rules, merchant_id=3002, note="\n[EWMA baseline 2]")
    eval_and_write(uid, rules, merchant_id=3001, note="\n[EWMA baseline 3]")
    eval_and_write(uid, rules, merchant_id=3002, note="\n[EWMA baseline 4]")
    eval_and_write(uid, rules, merchant_id=3001, note="\n[EWMA baseline 5]")
    #create a much larger than usual transaction to test EWMA logic
    eval_and_write(uid, rules, amount_cents=174000, merchant_id=3002, note="\n[EWMA spike]")
    
    wait_for_window()

    #Test 4: amount cap
    #try to make a high value transaction that is over the set amount cap
    print("\n4. Amount Cap Test.")
    print("High value transaction (€2,500) that should be above the set amount cap.\n")
    print("Please tap a card!")
    time.sleep(3)
    uid = wait_for_card(timeout=60)
    #reinstantiate and warm up rule objects to avoid extra flags in demo 
    rules = new_ruleset(uid)

    eval_and_write(uid, rules, amount_cents=250000, merchant_id=4001, note="\n[AmountCap]")

    wait_for_window()

    #Test 5: impossible travel
    #try two transactions at different zip codes to simulate impossible travel
    print("\n5. Impossible Travel Test.")
    print("Two transactions, one in Times Square, New York, the next in Santa Monica, Los Angeles")
    print("Note: The normal minimum time delta of 60 seconds is changed to 3 seconds for demonstration puposes.")
    
    #create two transactions with the same card from different locations (zip codes) to check impossible travel logic
    print("Please tap a card!")
    time.sleep(3)
    uid = wait_for_card(timeout=60)
    
    #reinstantiate and warm up rule objects to avoid extra flags in demo 
    rules = new_ruleset(uid)
    #reduce minimum time delta for demo to eliminate wait for second transaction
    rules.travel.min_dt_s = 3
    eval_and_write(uid, rules, merchant_id=5001, zipc=10036, note="\n[Travel A]")
    time.sleep(4)  # under 3s
    print("\nPlease tap the same card again!")
    time.sleep(3)
    uid = wait_for_card(timeout=60)
    eval_and_write(uid, rules, merchant_id=5002, zipc=90405, note="\n[Travel B]")

    #print confirmation that script is finished
    print("\nDone.")

if __name__ == "__main__":
    demo_walkthrough()