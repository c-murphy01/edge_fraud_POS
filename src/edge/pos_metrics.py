#import libraries and classes
import time, argparse, csv, json, os, statistics, sys, time, datetime, psutil, random
from collections import Counter

from edge.edge_rules import EdgeRules

#set path to log Tx records
LOG_PATH = "data/runs/pos_log.csv"

#function to measure CPU time to evaluate a transaction
def latency(args):
    #instantiate rules object
    rules = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")
    #warm up states for EWMA
    for _ in range(10): 
        #generate random transaction value from €1 - €40
        amount_cents = random.randint(100, 4000)
        tx = {"timestamp": int(time.time()),
        "merchant_id": 1234,
        "card_id": 4321,
        "amount": amount_cents / 100.0
        }
        rules.evaluate(tx)

    #number of iterations to run, passed in command line
    iters = int(args.iters)
    #empty list to keep smaples
    samples = []
    #start time
    #use perf_counter for high resolution timing
    t0 = time.perf_counter()
    #calculate time taken for each iteration and add to samples
    for _ in range(iters):
        t1 = time.perf_counter()
        rules.evaluate(tx)
        t2 = time.perf_counter()
        #in microseconds
        samples.append((t2-t1)*1e6)
    #total time is from start of loop to end in milliseconds
    t_total = (time.perf_counter() - t0) * 1e3

    #sort samples
    samples.sort()
    #finction to get time value at a gvien percentile
    def q(p):
        #get index for p in samples
        p_index = int(p/100*len(samples))-1
        #round the value and return
        r = round(samples[p_index], 2)
        return r

    #print iteration counter, total time, mean time, and 50th, 95th, and 99th percentiles    
    print(f"Iterations: {iters}  total: {t_total:.2f} ms  mean: {statistics.mean(samples):.2f} micro_s")
    print(f"P50: {q(50)} micro_s   P95: {q(95)} micro_s   P99: {q(99)} micro_s")

#function to show that all flags are explainable and are given with a reason
def explainability(args):
    #create variable for total logged transactions, flagged rows, and flags woth reason
    total = 0
    flagged = 0 
    with_reasons = 0
    #counter object
    counts = Counter()
    #open log file
    with open(LOG_PATH) as f:
        r = csv.DictReader(f)
        #loop through each transaction in log
        for row in r:
            #add to total count
            total += 1
            #check for a flag and for reasons and add to counts
            if int(row["flag"]) == 1:
                flagged += 1
                #separate multiple reasons
                reasons = [s for s in row["reasons"].split("|") if s]
                if reasons: with_reasons += 1
                counts.update(reasons)
    #print total tx and flagged
    print(f"Total tx: {total}  flagged: {flagged}")
    #calculate and print number of rows with flags
    if flagged:
        pct = 100.0 * with_reasons / flagged
        print(f"Explainable flags (flags with ≥1 reason): {pct:.1f}%")
        print("Top reasons:")
        #show the most common reasons
        for reason, cnt in counts.most_common(5):
            print(f"  {reason} --- {cnt}")
    else:
        print("No flagged transactions yet. Run the walkthrough first.")

#function to determine resources used (RSS) when evaluating a transaction.
def resource(args):
    #instantiate a rules object
    rules = EdgeRules(zip_csv_path="data/raw/zip_lat_long.csv")
    #warm up states
    for _ in range(10): 
        #generate random transaction value from €1 - €40
        amount_cents = random.randint(100, 4000)
        tx = {"timestamp": int(time.time()),
        "merchant_id": 1234,
        "card_id": 4321,
        "amount": amount_cents / 100.0
        }
        rules.evaluate(tx)
    #iteration number (from command line)
    iters = int(args.iters)

    #calculate time to evaluate tx for all iterations
    t1 = time.perf_counter()
    for _ in range(iters): rules.evaluate(tx)
    t2 = time.perf_counter()

    #get average time for Tx
    t_per_update = ((t2 - t1) / iters) * 1e6
    #print result
    print(f"CPU time per update: {t_per_update:.2f} micro_s (mean over {iters} iters)")

    #calculate the Resisdent State Size (portion of RAM occubied by the evaluation process)
    rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024*1024)
    #print result
    print(f"Process RSS: {rss_mb:.2f} MiB")

if __name__ == "__main__":
    #create a parser 
    p = argparse.ArgumentParser(prog="metrics")
    #add subcommands
    sub = p.add_subparsers(required=True)

    #register latency function as subcommand and add iteration argument
    s_lat = sub.add_parser("latency")
    s_lat.add_argument("--iters", type=int, default=20000)
    s_lat.set_defaults(func=latency)

    #register explainability subcommand
    s_exp = sub.add_parser("explainability")
    s_exp.set_defaults(func=explainability)

    #register resource use subcommand and add iteration argument
    s_res = sub.add_parser("resource")
    s_res.add_argument("--iters", type=int, default=200000)
    s_res.set_defaults(func=resource)

    #parse the command line args
    args = p.parse_args()
    #run function
    args.func(args) 