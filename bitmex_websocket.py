import websocket
import threading
import traceback
import time
import calendar
import json
import logging
import urllib
import math
from util.api_key import generate_nonce, generate_signature
from NamedPipe import NamedPipe

# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.
class BitMEXWebsocket:

    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self, namedpipe, endpoint, symbol, api_key=None, api_secret=None):
        '''Connect to the websocket and initialize data stores.'''
        self.logger = logging.getLogger(__name__)

        self.logger.debug("Initializing WebSocket.")
        self.endpoint = endpoint
        self.symbol = symbol
        self.pipe = namedpipe
        self.lastBid = 0.0
        self.lastAsk = 0.0

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.keys = {}
        self.exited = False

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        wsURL = self.__get_url()
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()

    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            nonce = generate_nonce()
            return [
                "api-nonce: " + str(nonce),
                "api-signature: " + generate_signature(self.api_secret, 'GET', '/realtime', nonce, ''),
                "api-key:" + self.api_key
            ]
        else:
            self.logger.info("Not authenticating.")
            return []

    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        #symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
        #genericSubs = ["margin"]

        symbolSubs = ["quote","tradeBin1m","order"]
        subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]

        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))

        return urllib.parse.urlunparse(urlParts)

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order', 'orderBookL2'} <= set(self.data):
            time.sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            time.sleep(0.1)

    def __send_command(self, command, args=None):
        '''Send a raw command.'''
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))

    def __update_tables(self, message, table, action):
        try:
            if 'subscribe' in message:
                self.logger.debug("Subscribed to %s." % message['subscribe'])
            elif action:

                if table not in self.data:
                    self.data[table] = []

                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                if action == 'partial':
                    self.logger.debug("%s: partial" % table)
                    self.data[table] += message['data']
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.
                    self.keys[table] = message['keys']
                elif action == 'insert':
                    self.logger.debug('%s: inserting %s' % (table, message['data']))
                    self.data[table] += message['data']

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBookL2'] and len(self.data[table]) > BitMEXWebsocket.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][int(BitMEXWebsocket.MAX_TABLE_LEN / 2):]

                elif action == 'update':
                    self.logger.debug('%s: updating %s' % (table, message['data']))
                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        item = findItemByKeys(self.keys[table], self.data[table], updateData)
                        if not item:
                            return  # No item found to update. Could happen before push
                        item.update(updateData)
                        # Remove cancelled / filled orders moved after message is send
                        #if table == 'order' and item['leavesQty'] <= 0:
                        #    self.data[table].remove(item)
                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, message['data']))
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = findItemByKeys(self.keys[table], self.data[table], deleteData)
                        self.data[table].remove(item)
                else:
                    raise Exception("Unknown action: %s" % action)
        except:
            self.logger.error(traceback.format_exc())

    def __on_message(self, ws, message):
        '''Handler for parsing WS messages.'''
        # display message before processing

        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None

        self.__update_tables(message, table, action)

        if(table == 'quote'):
            quote = self.data['quote'][-1]
            bid = float(quote['bidPrice'])
            ask = float(quote['askPrice'])
            if(bid != self.lastBid or ask != self.lastAsk):
                self.lastBid = bid 
                self.lastAsk = ask
                timestrct =  time.strptime(quote['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                timestamp = int(calendar.timegm(timestrct))
                self.pipe.Send("qt,{},{},{},{}".format(self.symbol,timestamp,bid,ask))
                self.logger.debug("Quote - symbol:{} time:{} bid:{} ask:{}".format(self.symbol,quote['timestamp'],bid,ask))
        elif(table == 'tradeBin1m'):
            candle = self.data['tradeBin1m'][-1]
            timestrct = time.strptime(candle['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp = int(calendar.timegm(timestrct))
            timestamp = int(timestamp/60) * 60 - 60
            volume = int(candle['volume'])/100000 + 1
            self.pipe.Send("cndl,{},{},{},{},{},{},{}".format(self.symbol,timestamp,candle['open'],candle['high'],candle['low'],candle['close'],volume))
            self.logger.debug("Candle - symbol:{} time:{} open:{} high:{} low:{} close:{} volume:{}".format(self.symbol,candle['timestamp'],candle['open'],candle['high'],candle['low'],candle['close'],volume))
        elif(table=='order'):
            orders = self.data['order']
            if(len(orders) > 0):
                self.pipe.Send("ordersupdt,{}".format(len(orders)))
                self.logger.info('----------------Orders----------------')
                remove_items = []
                for order in orders:
                    fmsg = "ordrtbl,{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(  order['orderID'],
                                                                                        order['clOrdID'],
                                                                                        order['clOrdLinkID'],
                                                                                        order['account'],
                                                                                        order['symbol'],
                                                                                        order['side'],
                                                                                        order['orderQty'],
                                                                                        order['price'],
                                                                                        order['ordType'],
                                                                                        order['ordStatus'],
                                                                                        order['triggered'],
                                                                                        order['leavesQty'],
                                                                                        order['text'],
                                                                                        order['transactTime'])
                    self.pipe.Send(fmsg)
                    self.logger.info(fmsg)
                    if (order['leavesQty'] <= 0):
                        remove_items.append(order)
                #Remove cancelled / filled orders
                for item in remove_items:
                    self.data['order'].remove(item)

    def __on_error(self, ws, error):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            raise websocket.WebSocketException(error)

    def __on_open(self, ws):
        '''Called when the WS opens.'''
        self.logger.debug("Websocket Opened.")

    def __on_close(self, ws):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')


# Utility method for finding an item in the store.
# When an update comes through on the websocket, we need to figure out which item in the array it is
# in order to match that item.
#
# Helpfully, on a data push (or on an HTTP hit to /api/v1/schema), we have a "keys" array. These are the
# fields we can use to uniquely identify an item. Sometimes there is more than one, so we iterate through all
# provided keys.
def findItemByKeys(keys, table, matchData):
    for item in table:
        matched = True
        for key in keys:
            if item[key] != matchData[key]:
                matched = False
        if matched:
            return item
