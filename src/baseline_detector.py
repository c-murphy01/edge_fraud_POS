from collections import defaultdict
import pandas as pd
import math

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

#simple class to create a cap for transaction amount
class AmountCap:
    def __init__(self, cap=500.0):
        self.cap = float(cap)

    def update(self, amount):
        amt = float(amount)
        #return flag if amount is greater than the cap
        return amt >= self.cap
    
#class to adapt amount rule per card based on Exponentially Weighted Moving Average on log(amount)
class CardEWMA:
    def __init__(self, alpha=0.2, k=3.0, initial=5, min_gate=None):
        #alpha is the smoothing factor, changes how reactive the model is
        self.alpha = float(alpha)
        #k is the threshold in standdard deviations
        self.k = float(k)
        #number of buckets to observe before producing flags
        self.initial = initial
        #minimum amount for flag
        self.min_gate = None if (min_gate == None) else float(min_gate)

        #mu is per card EWMA of the mean, E[x]
        self.mu = defaultdict(float)
        #the per card EWMA of the second moment, E[x^2] (to derive variance)
        self.mu2 = defaultdict(float)
        #per card count of buckets seen already
        self.seen = defaultdict(int)
    
    #update state for a card with latest transaction amount
    #returns (flag, info) where flag is true if z >= k and info is a log of z scores and log_amounts
    def update(self, card_id, amount):
        #convert amount to log(amount) so z scores are more comaparable across cards with different spend habits
        #take max of 0.01 and amount to avoid log(0) and negaitve numbers
        x = math.log(max(float(amount),0.01)) 

        a = self.alpha
        #how many buckets we've seen for this card
        s = self.seen[card_id]

        #get previous EWMA values
        mu_prev = self.mu[card_id]
        mu2_prev = self.mu2[card_id]

        #if enough buckets have been seen, calculate variance, std. dev. and z score
        if (s >= self.initial):
            var = max(mu2_prev-(mu_prev*mu_prev), 0.0)
            sigma = math.sqrt(var)

            #if std. dev, is too small, set z to 0, otherwise calc z score
            if sigma > 1e-8:
                z = (x - mu_prev) / sigma
            else:
                z = 0.0

            #handle no min gate safely
            gate_ok = True if self.min_gate is None else (amount >= self.min_gate)
            #rasie flag if z > k
            flag = (z >= self.k) and gate_ok

        #otherwise return no flag and allow 'warm up'
        else:
            z = 0.0
            flag = False
            
        #if this is the first tx seen, set mu to x and mu2 to x^2 to start
        if s == 0:
            mu = x
            mu2 = x*x
        #otherwise calculate with EWMA formula
        else:
            mu = a*x + (mu_prev*(1-a))
            mu2 = a * (x*x) + (mu2_prev*(1-a))

        #update variables for this card
        self.mu[card_id] = mu
        self.mu2[card_id] = mu2
        self.seen[card_id] = s + 1

        return flag, {"z": z, "log_amount": x}

#function to turn string to only digits for zip codes
def digits_only(s):
    return "".join(ch for ch in str(s) if ch.isdigit())

#function to fins haversine distance (distance on a sphere) for distance on the globe
def haversine_km(lat1, lon1, lat2, lon2):
    #earths radius
    R = 6371
    p = math.radians
    #find difference in lat and long in radians
    dlat = p(lat2 - lat1)
    dlon = p(lon2 - lon1)
    #apply haversine formula
    a = (math.sin(dlat/2)**2 + math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(dlon/2)**2)
    h_dist = 2 * R * math.asin(math.sqrt(a))
    #return haversine distance
    return h_dist

#class to create zip code -> (lat, long) lookup table
class ZipToCoord:
    def __init__(self, source):
        #read csv as dataframe
        df = pd.read_csv(source, dtype={"ZIP": str}, low_memory=False)

        #apply digits only function to all of zip column 
        df["ZIP"] = df["ZIP"].map(digits_only)

        #create dictionary of zip -> (lat, long)
        self._map = {z: (float(lat), float(lon))
                     for z, lat, lon in zip(df["ZIP"], df["LAT"], df["LNG"])}

    #fucntion to get coords for zip code
    def get(self, zip_code):
        #ensure no missing values
        if pd.isna(zip_code):
            return (None, None)
        #get only digits
        z = digits_only(zip_code)

        #return coords for zip code 'z' or None if z doesn't exist
        return self._map.get(z, (None, None))
    
#class to detect impossible travel distances/times between taps of a card
class ImpossibleTravel:
    def __init__(self, zip_lookup=None, vmax_kmh=500.0, min_km=100.0, min_dt_s=60.0):
        self.vmax = float(vmax_kmh)
        self.min_km = float(min_km)
        self.min_dt_s = float(min_dt_s)
        self.lookup = zip_lookup
        #dict for state of card (card_id -> (lat, long, timestamp))
        self.last = {}

    #return (lat, lon) as floats or None if unavailable
    def resolve_coords(self, zip_code=None, lat=None, lon=None):
        #if direct coords are given
        if lat is not None and lon is not None:
            return float(lat), float(lon)
        #otherwise try look up coords
        elif self.lookup is not None:
            return self.lookup.get(zip_code)
        #if neither above work return None
        return (None, None)

    def update(self, card_id, ts, zip_code=None, lat=None, lon=None):
        #parse ts as timestamp
        ts = pd.to_datetime(ts)

        #resolve coords for current tap
        cur_lat, cur_lon = self.resolve_coords(zip_code=zip_code, lat=lat, lon=lon)

        #if coords unresolved, store state and dont raise flag
        if cur_lat is None or cur_lon is None:
            self.last[card_id] = (ts, None, None)
            return False, {"reason": "no_coords", "speed_kmh": 0.0, "dist_km": 0.0, "dt_s": None}


        prev = self.last.get(card_id)
        #for the first tap, store state and dont raise flag
        if prev is None or prev[1] is None or prev[2] is None:
            self.last[card_id] = (ts, cur_lat, cur_lon)
            return False, {"reason": "no_prev", "speed_kmh": 0.0, "dist_km": 0.0, "dt_s": None}
        
        prev_ts, prev_lat, prev_lon = prev
    
        #calculate time gap
        dt_s = (ts - prev_ts).total_seconds()
        #ignore small gaps
        if dt_s <= self.min_dt_s:
            self.last[card_id] = (ts, cur_lat, cur_lon)
            return False, {"reason": "short_gap", "speed_kmh": 0.0, "dist_km": 0.0, "dt_s": dt_s}

        #calc distance
        dist_km = haversine_km(prev_lat, prev_lon, cur_lat, cur_lon)
        #ignore small distances
        if dist_km < self.min_km:
            self.last[card_id] = (ts, cur_lat, cur_lon)
            return False, { "reason": "short_dist", "speed_kmh": dist_km / (dt_s/3600.0), "dist_km": dist_km, "dt_s": dt_s}

        #calc implied speed
        speed_kmh = dist_km / (dt_s / 3600.0)
        #raise flag if above threshold
        flag = speed_kmh > self.vmax

        #update the state and return
        self.last[card_id] = (ts, cur_lat, cur_lon)
        return flag, {"reason": "impossible" if flag else "ok", "speed_kmh": speed_kmh, "dist_km": dist_km, "dt_s": dt_s}

#class to combine all of the edge rules set out above using logic OR into one edge flag
class RuleCombiner:
    def __init__(self, merchant_baseline, card_baseline, amount_cap, card_ewma, travel):
        self.m = merchant_baseline
        self.c = card_baseline
        self.ac = amount_cap
        self.ce = card_ewma
        self.travel = travel
    
    #tx will be a dict with keys from dataset (timestamp, merchant_id, card_id, amount, zip)
    #returen bool, True if any rule sets flag
    def update(self, tx):
        
        #list to collect flags from each rule
        flags = []
        
        #merchant velocity rule (unique cards per merchant/30s) - returns (flag, info)
        f, _ = self.m.update(tx["merchant_id"], tx["timestamp"], tx["card_id"])
        #add flag to flags list of rule fires
        flags.append(f)

        #card burst rule (merchants per card/30s) - returns (flag, info)
        f, _ = self.c.update(tx["card_id"], tx["timestamp"], tx["merchant_id"])
        flags.append(f)

        #amount cap rule
        flags.append(self.ac.update(tx["amount"]))

        #card EWMA amount rule - returns (flag, info)
        f, _ = self.ce.update(tx["card_id"], tx["amount"])
        flags.append(f)

        #location change rule
        f, _ = self.travel.update(tx["card_id"], tx["timestamp"], zip_code=tx.get("zip"), lat=tx.get("lat"), lon=tx.get("lon"))
        flags.append(f)

        #ruturns true if any rule fires
        return any(flags)