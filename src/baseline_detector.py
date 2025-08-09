from collections import defaultdict
import pandas as pd

#counter for how many unique cards one merchant sees in rolling 30s window
class MerchantWindow:
    #construct object set window to 30s
    def __init__(self, window_s=30, keep_windows=10):
        self.window = window_s
        self.keep_windows = keep_windows
        self.counts = defaultdict(set)

    #main method called for each tx
    #takes args merchant_id, timestamp, and card_id
    #returns current count of unique card_ids in this 30s window (after adding this card_id)
    def update(self, merchant_id, ts, card_id):
        #converts timestamp into bucket start time (pd.to_datetime ensure ts is a timestamp (not a string, etc.)
        bucket = pd.to_datetime(ts).floor(f"{self.window}s")
        #adds this card to the set for this merchant and bucket (cannot ahve duolicates so only new cards are added)
        key = (merchant_id, bucket)
        self.counts[key].add(card_id)
        #add small garbage collector to drop buckets after N windows
        cutoff = bucket - pd.Timedelta(seconds=self.window * self.keep_windows)
        #create list of keys for this merchant with bucket timestamps older than cutoff
        old_keys = [k for k in self.counts.keys() if k[0] == merchant_id and k[1] < cutoff]
        for k in old_keys:
            del self.counts[k]
        #return number of unique card_ids in thsi merchant/bucket set
        return len(self.counts[key])