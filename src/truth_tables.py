import pandas as pd
from pathlib import Path

#define constant path to processed data folder
PRO = Path("../data/processed")
#define constants for the ground truth files
TRUTH_M = pd.read_csv(PRO/"synth_truth_merchant.csv",
                      parse_dates=["bucket_30s"])
TRUTH_C = pd.read_csv(PRO/"synth_truth_card.csv",
                      parse_dates=["burst_start"])
TRUTH_M["merchant_id"] = TRUTH_M["merchant_id"].astype(str)
TRUTH_C["card_id"]     = TRUTH_C["card_id"].astype(str)

#make sets from merchant/bucket pairs
MERCHANT_SET = set(zip(TRUTH_M.merchant_id, TRUTH_M.bucket_30s))

#function to check if merchant/bucket pair is synthetic, returns True if synth/False if not
def is_merchant_spike(m_id, bucket):
    #ensure bucket is floored to avoid mismathces with detector
    bucket = pd.to_datetime(bucket).floor("30s")
    return (m_id, bucket) in MERCHANT_SET


CARD_SET = set(TRUTH_C.card_id)

#check if card number is synthetic, returns True if synth
def is_card_spike(card_id):
    return card_id in CARD_SET
