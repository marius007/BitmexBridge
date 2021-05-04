# Bitmex history download
import time
import bitmex
from NamedPipe import NamedPipe
from datetime import datetime

baseUrl = "https://www.bitmex.com/api/v1"
granularity = "1m"
requestBars = 750

class BitmexRestAPI:
    def __init__(self, pipe_price, pipe_order , TestServer, api_key, api_secret):
        self.__pipe_price = pipe_price
        self.__pipe_order = pipe_order
        self.client = bitmex.bitmex(test=TestServer, api_key=api_key, api_secret=api_secret)

    def DownloadHistory(self, pair):
        # Get current time
        try:
            response = self.client.Instrument.Instrument_get(symbol=pair, count=1, reverse=False).result()
        except Exception as e:
            print("Error in history request:" + e)
            return False

        # calculate history start time 
        timestamp = response[0][0]['timestamp'].timestamp()
        timestamp = (int(timestamp/60) * 60) - ((requestBars - 100) * 60)    
        startTime = datetime.utcfromtimestamp(timestamp) 
        # Download last 750 bars
        try:
            response = self.client.Trade.Trade_getBucketed(binSize=granularity,partial=False,symbol=pair,count=requestBars ,reverse=False, startTime=startTime).result()
        except Exception as e:
            print("Error in history request:" + e)
            return False

        for candle in response[0]:
            timestamp = candle['timestamp'].timestamp()
            timestamp = int(timestamp/60) * 60 - 60
            volume = int(candle['volume'])/100000 + 1
            self.__pipe_price.Send("cndl,{},{},{},{},{},{},{}".format(pair,timestamp,candle['open'],candle['high'],candle['low'],candle['close'],volume))

        return True

    def RestForward(self):
        while True:
            # read message from pipe
            message = self.__pipe_order.Receive()
            message = message.replace('\x00','')
            try:
                split  = message.split(',',1)
            except:
                time.sleep(0.5)
                continue;
            cmd = split[0]
            data = split[1]

            if(cmd == 'order'):
                # send rest request with the orders
                try:
                    result = self.client.Order.Order_newBulk(orders = data).result()
                except Exception as e:
                    print("Order exception:" + e)