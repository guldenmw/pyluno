import os
import logging
import inspect
import json
import time

from pprint import pprint, pformat

from websocket import create_connection, WebSocketTimeoutException

# from websocket import WebSocketConnectionClosedException

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# fh = logging.FileHandler('LunoStream.log')
# fh.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


class Stream(object):
    def __init__(self, key=None, secret=None, creds=None, path_to_creds=None, url=""):
        log.debug("\nInstantiating Luno Stream class object.\n")
        self.url = url if url else "wss://ws.luno.com/api/1/stream/"
        self.key = key
        self.__secret = secret

        self.bids = []
        self.asks = []
        self.trades = []
        self.addr = ''
        self.conn = None
        self.channels = {}

        if not key or not secret and not creds:
            self._handle_creds(path_to_creds)

        self._handle_configs()

    def _handle_creds(self, path_to_creds):
        if not path_to_creds:
            home = os.path.expanduser("~")
            path_to_creds = home + os.sep + "luno.json"

        with open(path_to_creds, "r") as read:
            creds = json.load(read)

        self.key = creds['k']
        self.__secret = creds['s']

    def start(self, pair="XBTZAR"):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        self.addr = self.url + pair

        log.info("\nConnecting to Luno Websocket for trading pair: '{}'.\n".format(pair))

        self._connect()

    def stop(self):
        if not self.conn:
            log.error("Connection is already closed!")
            return

        self.conn.close()
        self.conn = None

    def _handle_configs(self):
        from yaml import load
        with open("config.yaml", 'r') as read:
            configs = load(read)
        self.channels = configs['channels']

    def _connect(self):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        while self.conn is None:
            try:
                self.conn = create_connection(self.addr)

            except WebSocketTimeoutException:
                self.conn = None
                log.debug("\nCouldn't create websocket connection - retrying!\n")

        self.conn.send((json.dumps({'api_key_id': self.key, 'api_key_secret': self.__secret})))
        log.info("\nConnection Successful!\n")

    # def __format_bids(self, bids):
    #     function_name = inspect.stack()[0][3]
    #     log.debug(function_name)  # Log function name upon execution
    #
    #     for bid in bids:
    #         self.bids.update({bid['id']: {"price": bid['price'],
    #                                       "volume": bid['volume']}})
    #
    # def __format_asks(self, asks):
    #     function_name = inspect.stack()[0][3]
    #     log.debug(function_name)  # Log function name upon execution
    #
    #     for ask in asks:
    #         self.bids.update({ask['id']: {"price": ask['price'],
    #                                       "volume": ask['volume']}})

    def fetch_order(self, order_id):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        if order_id in self.bids:
            order = self.bids[order_id]
            location = "bids"
        elif order_id in self.asks:
            order = self.asks[order_id]
            location = "asks"
        else:
            return False, False

        return order, location

    def _update_order(self, new_order):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        order_id = new_order['order_id']
        old_order, location = self.fetch_order(order_id)

        if not old_order:
            return False

        log.debug("\nLocation: {}\nOrder: \n{}\n".format(location, old_order))

        new_volume = round(float(old_order['volume']) - float(new_order['base']), 6)

        if not new_volume:
            log.debug("\nOrder {} closed. Removing it from {}.\n".format(order_id, location))
            self._remove_order(order_id)
            return True

        log.debug("\nUpdating order {} volume to: {}\n".format(order_id, new_volume))

        new_info = {"volume": new_volume}

        eval("self.{}['{}'].update({})".format(location, order_id, new_info))
        return True

    def _handle_subscriptions(self, order, order_type):
        if order_type == "trade":
            if self.channels["order_book"] or self.channels["ticker"]:

        elif self.channels["order_book"]:
            return self.order_book

    def ticker(self):
        self.channels.update({"ticker": True)

    def trades(self):
        self.channels.update({"trades": True)

    def order_book(self):
        self.channels.update({"order_book": True)


    def _add_order(self, order):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        order_info = {order['create_update']['order_id']: {"price": order['create_update']['price'],
                                                           "volume": order['create_update']['volume']}}

        if order['create_update']['type'] == 'BID':
            self.bids.update(order_info)

        elif order['create_update']['type'] == 'ASK':
            self.asks.update(order_info)

        log.debug("\nOrder {} added to {}s.\n".format(order['create_update']['order_id'],
                                                      order['create_update']['type'].lower()))

        return True

    def _remove_order(self, order_id):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        order, location = self.fetch_order(order_id)

        try:
            eval("self.{}.pop('{}')".format(location, order_id))

        except IndexError:
            log.warning("\nOrder '{}' not found in self.{}.\n".format(order_id, location))
            return False
        log.debug("\nOrder {} removed from {}.\n".format(order_id, location))
        return True

    def _add_trade(self, trade, timestamp):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        self._update_order(trade)

        price = float(trade['counter']) / float(trade['base'])

        new_trade = {"id": trade['order_id'],
                     "price": price,
                     "volume": trade['base'],
                     "counter": trade["counter"],
                     "timestamp": timestamp}

        self.trades.append(new_trade)

        log.info("\nNew trade added: \n{}\n".format(pformat(new_trade)))
        return True

    def receive(self):
        return json.loads(self.conn.recv())

    def manager(self, order):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        log.debug("\nProcessing new order...\n")

        order_type = None

        # if "asks" in order:
            # log.info("\nFormatting bids and asks...\n")
            # self.__format_bids(order['bids'])
            # self.__format_asks(order['asks'])
            # order_type = "order_book"

        if "create_update" in order:
            if order["create_update"]:
                self._add_order(order)
                order_type = "create_order"

            elif order["delete_update"]:
                self._remove_order(order['delete_update']['order_id'])
                order_type = "delete_order"

            elif order['trade_updates']:
                for trade in order["trade_updates"]:
                    self._add_trade(trade, order['timestamp'])
                order_type = "trade"

        else:
            order_type = "order_book"

        if order_type:
            return self._handle_subscriptions(order, order_type)
