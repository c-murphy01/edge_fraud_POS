#import libraries and functions
import csv, time, os, datetime, argparse
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
    print(f"[demo] waiting {remaining}s to clear 30s window…")
    time.sleep(remaining)

#function to evaluater
def eval_and_write(uid, rules, amount_cents, merchant_id, zipc=None, lat=None, lon=None, note=""):
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

#function to run this demo script
def run():
    #tell user to tap card and leave it so the ID can be scannefd and the recent transactions can be read
    print("Place one card on the antenna and keep it there.")
    uid = wait_for_card(timeout=60)
    if not uid:
        print("No card detected."); return
    #show that the ID was read properly
    print("UID:", uid.hex())

    #instantiate a rules object
    rules = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")

    #warmup rules from card
    meta, recent = read_recent_tx(uid, max_count=30)
    rules.warmup_from_card(uid.hex(), list(reversed(recent)))

    #Test 1: card baseline 
    #create transactions with 3 different merchants in 30s
    eval_and_write(uid, rules, 199, merchant_id=1001, note="[CardBaseline 1/3]")
    eval_and_write(uid, rules, 199, merchant_id=1002, note="[CardBaseline 2/3]")
    eval_and_write(uid, rules, 199, merchant_id=1003, note="[CardBaseline 3/3]")

    #wait for the next 30w window so flags dont carry over
    wait_for_window()

    #Test 2: merchant baseline
    #3 different cards transacting with the same merchant in 30 seconds
    #adjust merchant threshold to 3 just for this test (easier to demo)
    rules.merchant.th = 3

    #transaction for card A
    print("Tap CARD A")
    uidA = wait_for_card(timeout=60)
    eval_and_write(uidA, rules, 199, merchant_id=9001, note="[Merchant A]")
    print("Card ID: ", uidA.hex())
    #sleep for a couple of seconds so user can change card
    time.sleep(2)

    #transaction for card B
    print("Now tap CARD B")
    uidB = wait_for_card(timeout=60)
    eval_and_write(uidB, rules, 199, merchant_id=9001, note="[Merchant B]")
    print("Card ID: ", uidB.hex())
    time.sleep(2)

    #transaction for card C
    print("Finally tap CARD C")
    uidC = wait_for_card(timeout=60)
    eval_and_write(uidC, rules, 199, merchant_id=9001, note="[Merchant C]")
    print("Card ID: ", uidC.hex())
    time.sleep(5)

    #wait for new window again
    wait_for_window()

    #reinstantiate and warm up rules object
    rules = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")
    meta, recent = read_recent_tx(uid, max_count=30)
    rules.warmup_from_card(uid.hex(), list(reversed(recent)))

    #Test 3: card EWMA
    #change initial tx needed for warm up to 5 for demo
    rules.ewma.initial = 5
    #create 5 transactions with 'normal amounts' for thsi card so thast there is a baseline for EWMA
    eval_and_write(uid, rules, 500, merchant_id=1200, note="[EWMA baseline]")
    eval_and_write(uid, rules, 400, merchant_id=1200, note="[EWMA baseline]")
    eval_and_write(uid, rules, 300, merchant_id=1200, note="[EWMA baseline]")
    eval_and_write(uid, rules, 400, merchant_id=1200, note="[EWMA baseline]")
    eval_and_write(uid, rules, 500, merchant_id=1200, note="[EWMA baseline]")
    #create a much larger than usual transaction to test EWMA logic
    eval_and_write(uid, rules, 174000, merchant_id=1200, note="[EWMA spike]")
    
    wait_for_window()

    #Test 4: amount cap
    #try to make a high value transaction that is over the set amount cap
    eval_and_write(uid, rules, 250000, merchant_id=1100, note="[AmountCap]")

    wait_for_window()

    #Test 5: impossible travel
    #reduce minimum time delta for demo to eliminate wait for second transaction
    rules.travel.min_dt_s = 3
    #create two transactions with the same card from different locations (zip codes) to check impossible travel logic
    eval_and_write(uid, rules, 199, merchant_id=1300, zipc=10001, note="[Travel A]")
    time.sleep(4)  # under 3s
    eval_and_write(uid, rules, 199, merchant_id=1300, zipc=90001, note="[Travel B]")

    #print confirmation that script is finished
    print("\nDone. Recent rows in data/runs/pos_log.csv will show flags & reasons.")

if __name__ == "__main__":
    run()