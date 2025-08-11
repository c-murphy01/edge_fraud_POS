from collections import defaultdict
import pandas as pd

#counter for how many unique cards one merchant sees in a 30s window
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

#counter for how many unique merchants one card sees in a 30s window
class CardWindow:
    def __init__(self, window_s=30, keep_windows=10):
        self.window = window_s
        self.keep_windows = keep_windows
        #for each card create a nested dictionary
        #outer dict -> key:card_id, value:inner dict
        #inner dict -> key:bucket timestamp, value:set of merchant_ids
        self.counts = defaultdict(lambda: defaultdict(set))

    def update(self, card_id, ts, merchant_id):
        #converts timestamp into bucket start time (pd.to_datetime ensure ts is a timestamp (not a string, etc.)
        ts = pd.to_datetime(ts)
        bucket = ts.floor(f"{self.window}s")
        #get/create dictionary for this card_id
        buckets = self.counts[card_id]
        #add this merchant id to set for this card and bucket
        buckets[bucket].add(merchant_id)

        #calculate oldest bucekt we keep's timestamp
        cutoff = bucket - pd.Timedelta(seconds=self.window * self.keep_windows)
        #find buckets older than cutoff bucket
        #wrap 'buckets.keys()' as list to allow deletion safely
        old_buckets = [b for b in list(buckets.keys()) if b < cutoff]
            
        #delete the old buckets
        for b in old_buckets:
            del buckets[b]

        #return number of unique merchant seen for this card/bucket
        return len(buckets[bucket])


### ADD COMMENTS BELOW HERE THEN DO EVAL_UTILS.py and FEATURES.py

#from baseline_detector import MerchantBaseline
#baseline rule wrapper class
class MerchantBaseline:
    def __init__(self, threshold=6, window_s=30, keep_windows=10):
        #threshold for how many cards per merchant/bucket before flag
        self.th = threshold
        #counter from above which counts unique cards per mechant/bucket
        self.win = MerchantWindow(window_s, keep_windows)

    def update(self, merchant_id, ts, card_id):
        #run the update methoid from window class above
        n = self.win.update(merchant_id, ts, card_id)
        #decide if the count crosses the threshold
        flag = n >= self.th
        #retuirm flag and some context for logs
        return flag, {"unique_cards": n, "bucket": pd.to_datetime(ts).floor(f"{self.win.window}s")}

class CardBaseline:
    def __init__(self, threshold=4, window_s=30, keep_windows=10):
        #thrshold for how many unique merchants per card/bucket before flag
        self.th = threshold
        #counter from above
        self.win = CardWindow(window_s, keep_windows)

    def update(self, card_id, ts, merchant_id):
        #run the update method from window class
        m = self.win.update(card_id, ts, merchant_id)
        #see if the count crosses threshold
        flag = m >= self.th
        #return flag and some context
        return flag, {"unique_merchants": m, "bucket": pd.to_datetime(ts).floor(f"{self.win.window}s")}
