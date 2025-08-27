#file to store recent transactions on an NFC card
#struct to pack/unpack integers to/from bytes
import struct
import time
import board
import busio
#library to use GPIO as digital input/output
from digitalio import DigitalInOut

from adafruit_pn532.spi import PN532_SPI

#initialise chip select (NSS) and reset pins as digital pins
#on WaveShare PN532 in SPI, NSS is on BCM4 (board.D4)
CS_PIN   = DigitalInOut(board.D4)
#RSTPDN on BCM20 (board.D20)
RESET_PIN= DigitalInOut(board.D20)
#initialise SPI bus using Pi's SPI pins
spi = busio.SPI(board.SCK, board.MOSI, board.MISO)

#create instance of PN532 driver
pn = PN532_SPI(spi, CS_PIN, reset=RESET_PIN, debug=False)
#enable MiFARE communication
pn.SAM_configuration()

#section to report memory usage of the POS loop
#try and except to avoid breaking loop
try:
    #import libs (_t to avoid clashes with variables)
    import os, psutil, gc, time as _t
    #build process for this process
    p = psutil.Process(os.getpid())
    #free unreachable objects
    gc.collect()
    #small sleep to wait out any jitter
    _t.sleep(0.05)
    #calculate RSS (convert to MiB)
    rss_mb = p.memory_info().rss / (1024*1024)
    #print results
    print(f"[resource] POS startup RSS: {rss_mb:.2f} MiB")
except Exception:
    pass

#set default key for MiFARE Classic authentication
KEY_A = bytes([0xFF]*6)

#block map 
#keep block 4 for header
HEADER_BLOCK = 4
#use all non-trailer blocks 5..62 as ring buffer for transactions
#skip every 4th block (trailer blocks (7,11,15,...))
#trailers contain keys and access bits, so we don't write to them
TX_BLOCKS = [b for b in range(4, 64) if ((b + 1) % 4) != 0]
#block 4 for header, leaves 44 blocks for transaction data
TX_DATA_BLOCKS = TX_BLOCKS[1:]

#HELPER FUNCTIONS

#calculate 16-bit checksum (2 byte masked sum)
def sum16(b):
    #calculate the 16-bit sum of the byte array
    return sum(b) & 0xFFFF

#function to try to authenticate a block with Key A, then Key B
#returns True if authentication succeeds with either key
def auth_any(uid, block):
    #AUTH_A = 0x60 (defined by PN532)
    return pn.mifare_classic_authenticate_block(uid, block, 0x60, KEY_A)

#function to return card's UID or timeout after a set time
def wait_for_card(timeout=None, stable_read=3):
    t0 = time.time()
    last = None
    count = 0
    while True:
        #try to read NFC card UID
        uid = pn.read_passive_target(timeout=0.2)
        #if a card is detected
        if uid:
            #if the UID read is stable (same UID read multiple times), return it
            if last is not None and uid == last:
                count += 1
            #if the UID read is different, reset count
            else:
                last = uid
                count = 1
            if count >= stable_read:
                return uid
        #if timeout is set and exceeded, return None
        if timeout and (time.time() - t0) > timeout:
            return None

def reselect_card(uid, attempts=3):
    #try to reselect the card by reading its UID multiple times
    for _ in range(attempts):
        #read the UID
        uid2 = pn.read_passive_target(timeout=0.2)
        #if UID is read and matches the original, return True
        if uid2 and uid2 == uid:
            return True
    #else return False
    return False

#function to read a block from the card, authenticating first
def read_block(uid, block, attempts=3):
    #try to authenticate and read the block multiple times
    for _ in range(attempts):
        #if auth succeeds, read the block and return it
        if auth_any(uid, block):
            data = pn.mifare_classic_read_block(block)
            if data:
                return data
        #if auth fails, try to reselect the card and try again
        reselect_card(uid)
        #wait a bit before retrying
        time.sleep(0.1)
    #if all attempts fail, return None
    return None

#function to write a block (16 bytes) to the card, authenticating first
def write_block(uid, block, data, attempts=3):
    if len(data) != 16:
        raise ValueError("Data must be exactly 16 bytes")
    for _ in range(attempts):
        #if auth succeeds, write the block and return True if successful
        if auth_any(uid, block):
            if pn.mifare_classic_write_block(block, data):
                return True
        #if auth fails, try to reselect the card and try again
        reselect_card(uid)
        #wait a bit before retrying
        time.sleep(0.1)
    #if all attempts fail, return False
    return False

#HEADER FUNCTIONS and PACKING/UNPACKING

#fucntion to pack header data into a byte array
def pack_header(version, write_index, total_count, last_timestamp):
    #create a bytearray of 16 bytes and fill it with header data
    b = bytearray(16)
    #set magic header bytes
    b[0:4] = b"COLM"
    #set version
    b[4] = version & 0xFF
    #set write index
    b[5] = write_index & 0xFF
    #set total count (big endian 16-bit unsigned integer)
    b[6:8] = struct.pack(">H", total_count & 0xFFFF)
    #set last timestamp (big endian 32-bit unsigned integer)
    b[8:12] = struct.pack(">I", last_timestamp & 0xFFFFFFFF)
    #set unused bytes to zero
    b[12] = 0
    b[13] = 0
    #set checksum (16-bit sum of first 14 bytes)
    b[14:16] = struct.pack(">H", sum16(b[:14]))
    #return the packed byte array
    return bytes(b)

#function to unpack header data from a byte array
def unpack_header(b):
    #check if the byte array is valid
    if not b or len(b) != 16 or b[0:4] != b"COLM":
        return None
    #check if the checksum is valid
    #struct.unpack returns a tuple, so add [0] to get the first element
    if sum16(b[:14]) != struct.unpack(">H", b[14:16])[0]:
        return None
    #if both checks succeed, return a dictionary with the unpacked header data
    return {
        "version": b[4],
        "write_index": b[5],
        "total_count": struct.unpack(">H", b[6:8])[0],
        "last_timestamp": struct.unpack(">I", b[8:12])[0]
    }

#function to pack a transaction into a byte array
def pack_tx(timestamp, amount_cents, merchant_id, flags, zip_code=None):
    #create a bytearray of 16 bytes and fill it with transaction data
    b = bytearray(16)
    #set timestamp (big endian 32-bit unsigned integer)
    b[0:4] = struct.pack(">I", timestamp & 0xFFFFFFFF)
    #set amount in cents (big endian 32-bit signed integer)
    b[4:8] = struct.pack(">i", amount_cents)
    #set merchant ID (big endian 16-bit unsigned integer)
    b[8:10] = struct.pack(">H", merchant_id & 0xFFFF)
    #set zip code (big endian 16-bit unsigned integer, 0 if no zip code)
    #initalise zip_int to 0
    zip_int = 0
    if zip_code:
        #convert zip code to string, keeping only 5 digits
        z = "".join(ch for ch in str(zip_code) if ch.isdigit())[:5]
        if z:
            #convert to integer, ensuring it's within max unsigned int range 
            zip_int = max(0, min(65535, int(z)))
    b[10:12] = struct.pack(">H", zip_int)
    #set flags (1 byte)
    b[12] = flags & 0xFF
    #set unused bytes to zero
    b[13] = 0
    #set checksum (16-bit sum of first 14 bytes)
    b[14:16] = struct.pack(">H", sum16(bytes(b[:14])))
    #return the packed byte array
    return bytes(b)

#function to unpack a transaction from a byte array
def unpack_tx(b):
    #check if the byte array is valid
    if not b or len(b) != 16:
        return None
    #check if the checksum is valid
    if sum16(b[0:14]) != struct.unpack(">H", b[14:16])[0]:
        return None
    #if both checks succeed, continue
    #unpack zip code
    zip_int = struct.unpack(">H", b[10:12])[0]
    #fill with 0s until it is 5 digits long if needs be
    #set string to None if zip_int is 0
    zip_str = str(zip_int).zfill(5) if zip_int > 0 else None
    #return a dictionary with the unpacked transaction data
    return {
        "timestamp": struct.unpack(">I", b[0:4])[0],
        "amount_cents": struct.unpack(">i", b[4:8])[0],
        "merchant_id": struct.unpack(">H", b[8:10])[0],
        "zip": zip_str,
        "flags": b[12]
    }

#HIGH LEVEL FUNCTIONS

#function to read the header block and return the metadata
def read_header(uid):
    #read the header block (block 4)
    header_block = read_block(uid, HEADER_BLOCK)
    #if reading the block fails, return None
    if not header_block:
        return None
    #unpack the header data from the block
    return unpack_header(header_block)

#function to write the header block with metadata
def write_header(uid, metadata):
    #pack the header data into a byte array
    header_data = pack_header(
        metadata["version"],
        metadata["write_index"],
        metadata["total_count"],
        metadata["last_timestamp"]
    )
    #write the packed header data to the header block
    return write_block(uid, HEADER_BLOCK, header_data)

#function to load header, or create it if it doesn't exist
def load_init_header(uid):
    #try to read the header block
    metadata = read_header(uid)
    #if header exists, return it
    if metadata:
        return metadata
    #if header does not exist, create a new one with default values
    metadata = {"version": 1, "write_index": 0, "total_count": 0, "last_timestamp": 0}
    #try to write the new header to the card
    if write_header(uid, metadata):
        #wait a bit to ensure the write is stable
        time.sleep(0.1)
        reselect_card(uid)
        return metadata
    #if that fails, return None
    else:
        return None
    
#function to read recent transactions from the card
#returns the metadata and a list of transactions (newest first)
def read_recent_tx(uid, max_count=10):
    #load or create the header
    metadata = load_init_header(uid)
    #if header is None, return None and an empty list
    if not metadata:
        return None, []
    #get the write index and create a list to hold transactions
    index = metadata["write_index"]
    transactions = []
    #get amount of transactions to read
    to_read = min(max_count, len(TX_DATA_BLOCKS), metadata["total_count"])
    #loop through the last transactions in reverse order
    for i in range(to_read):
        #calculate the block number to read
        block_index = (index - 1 - i) % len(TX_DATA_BLOCKS)
        #set block to read from
        block = TX_DATA_BLOCKS[block_index]
        #read the transaction block
        bytes_read = read_block(uid, block)
        #unpack the transaction data
        tx = unpack_tx(bytes_read) if bytes_read else None
        #if transaction data is valid, add it to the list
        if tx:
            transactions.append(tx)

    #return the metadata and the list of transactions
    return metadata, transactions

#function to write a transaction to the card
def write_recent_tx(uid, tx):
    #load or create the header
    metadata = load_init_header(uid)
    #if header is None, return False
    if not metadata:
        return False, "header not found"
    
    #get the block to write to with current write index
    block = TX_DATA_BLOCKS[metadata["write_index"] % len(TX_DATA_BLOCKS)]

    #try to write the transaction data to the block, if it fails, return False and an error message
    if not write_block(uid, block, tx): 
        return False, "write failed"
    #if write succeeds, update the metadata
    #increment write index, wrap back to start if it exceeds the number of data blocks
    metadata["write_index"] = (metadata["write_index"] + 1) % len(TX_DATA_BLOCKS)
    #increment total count, but limit it to 65535 (2 byte unsigned integer max)
    metadata["total_count"] = min(65535, metadata["total_count"] + 1)
    #set the last timestamp to current time
    metadata["last_timestamp"] = int(time.time())

    #only update header every 3 transactions to speed up process
    UPDATE_HEADER_EVERY = 3
    if (metadata["total_count"] % UPDATE_HEADER_EVERY) == 0:
        #try to write the updated metadata back to the header block
        if not write_header(uid, metadata):
            return False, "header write failed"
    else:
        pass

    #if everything succeeds, return True and a success message
    return True, "write successful"

#CLEAR / FORMAT FUNCTIONS

#function to clear the ring buffer by writing empty blocks
def clear_ring_buffer(uid):
    #create a payload of 16 bytes of zeros
    payload = bytes([0] * 16)
    cleared = 0
    #loop through all transaction data blocks
    for b in TX_DATA_BLOCKS:
        #try to write an empty block
        if write_block(uid, b, payload):
            #increment counter
            cleared += 1
    #return the number of blocks cleared
    return cleared

#function to reset the header, keeping the magic bytes
def reset_header(uid, version=1):
    #create a new header with default values
    metadata = {
        "version": version,
        "write_index": 0,
        "total_count": 0,
        "last_timestamp": 0
    }
    return write_header(uid, metadata)

#function to clear the header including the magic bytes
def clear_header(uid):
    #clear the header block by writing all zeros
    return write_block(uid, HEADER_BLOCK, b"\x00"*16)

#function to format card for use, clear data blocks and reset or clear header
def format_card(uid, keep_header=True):
    #clear the ring buffer
    cleared = clear_ring_buffer(uid)
    #if keep_header is True, reset the header (keep magic, reset counters)
    if keep_header:
        reset_header(uid)
        return True, f"cleared {cleared} data blocks; header reset"
    #if keep_header is False, clear header completely
    else:
        clear_header(uid)
        return True, f"cleared {cleared} data blocks; header cleared"
