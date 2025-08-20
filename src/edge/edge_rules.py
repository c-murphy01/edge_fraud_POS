import os, sys
#add src directory to path so we can import edge_card and edge_rules
#THIS DIR is the directory of this file (~/edge_fraud_POS/src/edge)
THIS_DIR = os.path.dirname(__file__)
#SRC_DIR is the src directory, one level up from THIS_DIR (~/edge_fraud_POS/src)
SRC_DIR = os.path.normpath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from datetime import datetime
import pandas as pd
#import rules from baseline_detector
from baseline_detector import (
    MerchantBaseline, CardBaseline, AmountCap, CardEWMA,
    ImpossibleTravel, ZipToCoord
)

#class to hold edge rules and their parameters
#parameters set based on tuned params from baseline evaluation notebook
class EdgeRules:
    def __init__(
        self, 
        merchant_threshold=6,
        card_threshold=3,
        amount_cap=1500,
        ewma_alpha=0.2,
        ewma_k=5.25,
        ewma_initial=10,
        ewma_min_gate=850,
        zip_csv_path=None,
        travel_vmax_kmh=600.0,
        travel_min_km=150.0,
        travel_min_dt_s=60.0,
    ):

        #initialize detectors with their parameters
        self.merchant = MerchantBaseline(threshold=merchant_threshold, window_s=30, keep_windows=10)
        self.card = CardBaseline(threshold=card_threshold, window_s=30, keep_windows=10)
        self.cap = AmountCap(cap=amount_cap)
        self.ewma = CardEWMA(alpha=ewma_alpha, k=ewma_k, initial=ewma_initial, min_gate=ewma_min_gate)
        lookup = ZipToCoord(source=zip_csv_path) if zip_csv_path else None
        self.travel = ImpossibleTravel(zip_lookup=lookup, vmax_kmh=travel_vmax_kmh, 
                                        min_km=travel_min_km, min_dt_s=travel_min_dt_s)
    
    #function to convert a timestamp integer to a pandas datetime object
    def pd_timestamp(self, ts_int):
        return pd.to_datetime(int(ts_int), unit="s")
    
    #function to rebuild state from a cards last few transactions
    #replays card history to detectors so they are ready for new transactions
    def warmup_from_card(self, card_uid_hex, records):
        #loop through each record in the card history
        for r in records:
            #convert timestamp to pandas datetime
            ts = self.pd_timestamp(r["timestamp"])
            #convert amount from cents to float currency
            amt = r["amount_cents"] / 100.0
            #get merchant ID and card ID from record
            merch_id = r["merchant_id"]
            card_id = card_uid_hex

            #update each detector's state with the transaction
            #don't check return flags, just warm up the detectors
            self.merchant.update(merch_id, ts, card_id)
            self.card.update(card_id, ts, merch_id)
            self.cap.update(amt)
            self.ewma.update(card_id, amt)
            self.travel.update(card_id, ts, zip_code=None, lat=None, lon=None)
    
    #function to evaluate a transaction against the edge rules
    def evaluate(self, tx):
        #create a list to hold reasons for fraud
        reasons = []

        #set variables from transaction data
        ts = self.pd_timestamp(tx["timestamp"])
        merch_id = tx["merchant_id"]
        card_id = tx["card_id"]
        amt = float(tx["amount"])
        zipc = tx.get("zip")
        lat = tx.get("lat")
        lon = tx.get("lon")
        
        #check each detector and add reasons if fraud is detected
        f, _ = self.merchant.update(merch_id, ts, card_id)
        if f: reasons.append("merchant_window")

        f, _ = self.card.update(card_id, ts, merch_id)
        if f: reasons.append("card_window")

        if self.cap.update(amt):
            reasons.append("amount_cap")

        f, _ = self.ewma.update(card_id, amt)
        if f: reasons.append("card_ewma")

        f, info = self.travel.update(card_id, ts, zip_code=zipc, lat=lat, lon=lon)
        if f: reasons.append(f"impossible_travel_{int(info.get('speed_kmh',0))}kmh")

        #if any reasons were added, return True and the reasons list
        return (len(reasons) > 0), reasons