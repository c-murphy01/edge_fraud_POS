#import libraries, classe and functiosn
import time, statistics
from time import perf_counter_ns
from edge.edge_rules import EdgeRules
from edge.edge_card import wait_for_card, read_recent_tx, write_recent_tx, pack_tx

#helper funcs
#nanosecond timer
def ns(): return perf_counter_ns()
#convert to micro seconds
def us(dt_ns): return dt_ns / 1e3
#convert to milliseconds
def ms(dt_ns): return dt_ns / 1e6

#main function to test pos loop
def main(iters=20):
    #ask user to present card
    print("Place and KEEP a card on the antenna for the duration.")
    #get uid
    uid = wait_for_card(timeout=10)
    if not uid:
        print("No card."); return
    uid_hex = uid.hex()
    #instantiate rules
    rules = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")

    #warm up from card
    meta, recent = read_recent_tx(uid, max_count=10)
    rules.warmup_from_card(uid_hex, list(reversed(recent)))

    #lists to store times
    eval_us = []
    read_ms = []
    write_ms = []
    total_ms = []

    #for each iteration
    for i in range(iters):
        #start time
        t0 = ns()

        #check read time
        t_r0 = ns()
        meta, recent = read_recent_tx(uid, max_count=10)
        t_r1 = ns()

        #warm up rules from history
        rules.warmup_from_card(uid_hex, list(reversed(recent)))

        #time evaluation of transaction
        tx_ts = int(time.time())
        amount_cents = 999
        tx = {
            "timestamp": tx_ts,
            "merchant_id": 1234,
            "card_id": uid_hex,
            "amount": amount_cents / 100.0,
            "zip": None, "lat": None, "lon": None
        }
        t_e0 = ns()
        flag, reasons = rules.evaluate(tx)
        t_e1 = ns()

        #time writing tx to cards
        rec = pack_tx(tx_ts, amount_cents, tx["merchant_id"], 1 if flag else 0)
        t_w0 = ns()
        ok, _ = write_recent_tx(uid, rec)
        t_w1 = ns()

        #stop time
        t1 = ns()

        #add timings to lists
        read_ms.append(ms(t_r1 - t_r0))
        eval_us.append(us(t_e1 - t_e0))
        write_ms.append(ms(t_w1 - t_w0))
        total_ms.append(ms(t1 - t0))

    #fucntion to sort and index lists
    def q(arr, p):
        #sort the input list
        arr = sorted(arr)
        #get index for percentile p
        idx = max(0, min(len(arr)-1, int(p/100*len(arr))-1))
        #return value at index
        return arr[idx]

    #print all results
    #numbner of iterations
    print(f"\nIters: {iters}")
    #mean and 50th/95th percentile for read timing
    print(f"Read  (ms): mean={statistics.mean(read_ms):.2f}  P50={q(read_ms,50):.2f}  P95={q(read_ms,95):.2f}")
    #mean and 50th/95th percentile for evaluation timing
    print(f"Eval  (µs): mean={statistics.mean(eval_us):.2f}  P50={q(eval_us,50):.2f}  P95={q(eval_us,95):.2f}")
    #mean and 50th/95th percentile for write timing
    print(f"Write (ms): mean={statistics.mean(write_ms):.2f}  P50={q(write_ms,50):.2f}  P95={q(write_ms,95):.2f}")
    #mean and 50th/95th percentile for total timing
    print(f"TOTAL (ms): mean={statistics.mean(total_ms):.2f}  P50={q(total_ms,50):.2f}  P95={q(total_ms,95):.2f}")

if __name__ == "__main__":
    #call main function with 30 iterations
    main(iters=30)
