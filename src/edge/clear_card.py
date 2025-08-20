import sys
import argparse
from edge.edge_card import wait_for_card, format_card

def main():
    #parse command line arguments
    parser = argparse.ArgumentParser(description="Clear the transaction history on an NFC card and reset header")
    #add argument to clear the header instead of resetting it
    parser.add_argument("--clear", action="store_true", help="Clear header (remove magic) instead of resetting it")
    args = parser.parse_args()

    #wait for a card to be tapped
    print("Tap card to clear/format … keep it still until done.")
    uid = wait_for_card(timeout=60)
    #if no card is detected, print a message and exit
    if not uid:
        print("No card detected.")
        sys.exit(1)
    #if card is detected, reset or clear it
    ok, info = format_card(uid, keep_header=not args.clear)
    #print result
    print("OK" if ok else "FAILED", "-", info)

if __name__ == "__main__":
    main()