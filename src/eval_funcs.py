#helper funcs for threshold and evaluation metrics

import pandas as pd

# Turn a dictionary (merchant_id/bucket:count for merchant spike) into a set of predicted pairs
def threshold_predictions(counts, threshold):
    #returns pair if c meets the threshold
    return {pair for pair, c in counts.items() if c >= threshold}

#confusion metric counters per bucket
#args are predicted (set of pairs flagged), truth (set of true spike pairs), 
#and universal (set of all covered pairs)
def per_bucket_confusion(predicted, truth, universal):
    #true positive
    tp = len(predicted & truth)
    #false positive
    fp = len(predicted - truth)
    #false negative
    fn = len(truth - predicted)
    #true negative
    tn = len((universal - truth) - predicted)
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn}

#function to calc precision, recall and f1 scores
def precision_recall_f1(tp, fp, fn):
    #use safe division to avoid un expected errors
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}

#check various thresholds from args start to stop (inclusive)
def sweep_thresholds(counts, truth_pairs, universal_pairs, start, stop):
    results = []
    #for each threshold value
    for th in range(start, stop + 1):
        pred = threshold_predictions(counts, th)
        cm = per_bucket_confusion(pred, truth_pairs, universal_pairs)
        m = precision_recall_f1(cm["tp"], cm["fp"], cm["fn"])
        row = {"th": th}
        #add confusion matrix values to row
        row.update(cm)
        #add prec,recall, and f1 scores to row
        row.update(m)
        #add row to results
        results.append(row)
    return results
