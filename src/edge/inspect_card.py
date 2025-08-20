import datetime
#import card read functions
from edge.edge_card import wait_for_card, read_recent_tx, read_block, HEADER_BLOCK

#function to format timestamps for display
def format_ts(ts):
    #instantiate a datetime object
    dt_obj = datetime.datetime
    #convert timestamp to a datetime object
    dt_from_ts = dt_obj.fromtimestamp(ts)
    #format the datetime object to be readable
    formatted = dt_from_ts.strftime("%Y-%m-%d %H:%M:%S")
    #return the formatted string
    return formatted

#main function to inspect the card
def main():
    #wait for a card to be tapped and get its UID
    print("Tap card to inspect...")
    uid = wait_for_card(timeout=30)
    if not uid:
        print("No card detected.")
        return
    print("UID:", uid.hex())

    #read 10 most recent transactions from the card
    meta, recent = read_recent_tx(uid, max_count=10)
    #if read fails, print an error message and return
    if not meta:
        print("No header / init failed.")
        return

    #print the card header information
    print("Header:", meta)

    #print recent transactions from most recent to oldest
    print("\nRecent records (newest first):")
    for r in recent:
        #format and print each transaction record
        print(f"  {format_ts(r['timestamp'])}  amount_cents={r['amount_cents']}  "
              f"merchant={r['merchant_id']}  flags={r['flags']}")

if __name__ == "__main__":
    main()