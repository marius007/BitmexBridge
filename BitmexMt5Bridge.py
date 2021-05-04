# Bitmex MT5 bridge
import logging
import threading
from time import sleep
from NamedPipe import NamedPipe
from bitmex_rest import BitmexRestAPI
from bitmex_websocket import BitMEXWebsocket

enablePipe = True
TestServer = True
websocketSymbol = 'XBTUSD'


# Live Account
websocketUrl_live = 'wss://www.bitmex.com/realtime'
API_ID_live = 'xxxxxxxxxxxxxxxxxxxxxx'
API_SECRET_live = 'xxxxxxxxxxxxxxxxxx'

# Testnet account
websocketUrl_demo = 'https://testnet.bitmex.com/api/v1'
API_ID_demo = 'xxxxxxxxxxxxxxxxxxxxxxxx'
API_SECRET_demo = 'xxxxxxxxxxxxxxxxxxxx'

# Basic use of websocket.
def run():
    logger = setup_logger()

    if(TestServer):
        websocketUrl = websocketUrl_demo
        API_ID = API_ID_demo
        API_SECRET = API_SECRET_demo

    else:
        websocketUrl = websocketUrl_live
        API_ID = API_ID_live
        API_SECRET = API_SECRET_live

    # connect to named pipe
    logger.info("Wait for MT5 pipes....")
    pipe_price = NamedPipe('Bitmex.Pipe.ServerPrice',enablePipe)
    pipe_order = NamedPipe('Bitmex.Pipe.ServerOrder',enablePipe)
    pipe_price.Connect()
    pipe_order.Connect()
    logger.info("Price Pipe is :".format(pipe_price.GetStatus()) )
    logger.info("Order Pipe is :".format(pipe_order.GetStatus()) )

    logger.info("login to rest api account")
    restApi = BitmexRestAPI(pipe_price,pipe_order,TestServer,API_ID,API_SECRET)

    logger.info("Download history")
    restApi.DownloadHistory(websocketSymbol)

    logger.info("Create rest mesage forward thread")
    rest_thread = threading.Thread(target=restApi.RestForward)
    rest_thread.start()

    logger.info("Instantiating the WS and make it connect.")
    ws = BitMEXWebsocket(namedpipe=pipe_price, endpoint=websocketUrl, symbol=websocketSymbol,
                         api_key=API_ID, api_secret=API_SECRET)

    while(ws.ws.sock.connected):
        logger.info("Connection is active.")
        sleep(300)

def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


run()