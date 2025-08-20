#file to verify the PN532 SPI setup is correct and read an NFC card's UID

#import helpers for raspberry pi from Blinka
import board
import busio
#library to use GPIO as digital input/output
from digitalio import DigitalInOut
#library for PN532 SPI communication (avoids clock stretching issues with i2c)
from adafruit_pn532.spi import PN532_SPI

#initialise SPI bus using Pi's SPI pins
spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
#initialise chip select (NSS) and reset pins as digital pins
#on WaveShare PN532 in SPI, NSS is on BCM4 (board.D4)
cs_pin = DigitalInOut(board.D4) 
#RSTPDN on BCM20 (board.D20)
rst_pin = DigitalInOut(board.D20)
#initialise PN532 SPI object
pn = PN532_SPI(spi, cs_pin, reset=rst_pin, debug=False)
#put PN532 into reader mode, allow MiFARE communication
pn.SAM_configuration()

#loop until a card is detected, then print its UID
print("Waiting for NFC card…")
while True:
    #try to read NFC card UID
    uid = pn.read_passive_target(timeout=0.5)
    #if a card is detected, print its UID
    if uid:
        print("UID:", uid.hex())
        break
