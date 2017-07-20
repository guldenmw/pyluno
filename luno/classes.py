import os
import logging
from threading import Thread
import inspect
import json
import time
import sys
import hashlib

from pprint import pprint, pformat

from websocket import create_connection, WebSocketTimeoutException

# from websocket import WebSocketConnectionClosedException

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


# fh = logging.FileHandler('LunoStream.log')
# fh.setLevel(logging.DEBUG)
# log.setLevel(logging.INFO)


class Stream(object):
    def __init__(self, key=None, secret=None, creds=None, path_to_creds=None, url=""):
        log.debug("\nInstantiating Luno Stream class object.\n")
        self.url = url if url else "wss://ws.luno.com/api/1/stream/"
        self.key = key
        self.__secret = secret

        self.bids = None
        self.asks = None
        self._trade_history = []

        self._trades = None
        self._ticker = None
        self._order_book = None

        self.addr = None
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
        import definitions
        from yaml import load

        config_path = os.path.join(definitions.MODULE_ROOT, 'config.yaml')
        with open(config_path, 'r') as read:
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

    def _update_order(self, new_order):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        order_id = new_order['order_id']
        old_order, index, location = self.fetch_order(order_id)

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

        eval("self.{}[{}].update({})".format(location, index, new_info))
        return True

    def _add_order(self, order):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        order_info = {"id": order['create_update']['order_id'],
                      "price": order['create_update']['price'],
                      "volume": order['create_update']['volume']}
        previous_index = 0

        if order['create_update']['type'] == 'BID':
            previous = [i for i in self.bids if float(i['price']) <= float(order_info['price'])]
            if previous:
                previous_index = self.bids.index(previous[-1]) + 1

            self.bids.insert(previous_index, order_info)

        elif order['create_update']['type'] == 'ASK':
            previous = [i for i in self.asks if float(i['price']) <= float(order_info['price'])]
            if previous:
                previous_index = self.asks.index(previous[-1]) + 1

            self.asks.insert(previous_index, order_info)

        log.debug("\nOrder {} added to {}s.\n".format(order['create_update']['order_id'],
                                                      order['create_update']['type'].lower()))

        self._handle_subscriptions("order_book")

        return True

    def _remove_order(self, order_id):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        order, index, location = self.fetch_order(order_id)

        try:
            eval("self.{}.pop({})".format(location, index))

        except IndexError:
            log.warning("\nOrder '{}' not found in self.{}.\n".format(order_id, location))
            return False

        self._handle_subscriptions("order_book")

        log.debug("\nOrder {} removed from {}.\n".format(order_id, location))
        return True

    def __trade_hash(self, trade):
        trade_string = "".join(sorted([str(i) for i in trade.values()])).encode('utf-8')
        return hashlib.md5(trade_string).hexdigest()

    def _add_trade(self, trade, timestamp):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        # for trade in trades:
        self._update_order(trade)

        price = float(trade['counter']) / float(trade['base'])

        new_trade = {"id": trade['order_id'],
                     "price": price,
                     "volume": trade['base'],
                     "counter": trade["counter"],
                     "timestamp": timestamp}

        trade_hash = self.__trade_hash(new_trade)

        new_trade['hash'] = trade_hash

        self._trade_history.insert(0, new_trade)

        self._trades = new_trade
        self._ticker = new_trade['price']

        self._handle_subscriptions("trade")

        log.info("\nNew trade added: \n{}\n".format(pformat(new_trade)))
        return True

    def _handle_subscriptions(self, order_type):
        if order_type == "trade":
            if self.channels["ticker"]:
                if self._trade_history:
                    payload = json.dumps(round(self._trade_history[0]['price'], 0))
                    print(payload)

            if self.channels["trades"]:
                if self._trades:
                    payload = json.dumps(self._trades)
                    print(payload)

            if self.channels["trade_history"]:
                if self._trade_history:
                    payload = json.dumps(self._trade_history)
                    print(payload)

        elif self.channels["order_book"]:
            if self._order_book:
                payload = json.dumps(self._order_book)
                print(payload)

    def _subscription_thread(self):
        self.manager()

    def subscribe(self, channel):
        self.channels[channel] = True
        sub_thread = Thread(target=self._subscription_thread)
        sub_thread.start()

    def get_available_channels(self):
        channels = [i for i in self.channels.keys()]
        print("Available channels: ")
        for channel in channels:
            print(channel)
        print("\n")

        return channels

    def ticker(self):
        self.manager()
        return self._ticker

    def trades(self):
        self.manager()
        return self._trades

    def order_book(self):
        self.manager()
        return self._order_book

    def trade_history(self):
        self.manager()
        return self._trade_history

    def fetch_order(self, order_id):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        bids_test = [i for i in self.bids if i['id'] == order_id]
        asks_test = [i for i in self.asks if i['id'] == order_id]
        log.debug("\nbids_test: \n{}\n\nasks_test: \n{}\n".format(bids_test, asks_test))

        if bids_test:
            index = self.bids.index(bids_test[0])
            order = self.bids[index]
            location = "bids"
        elif asks_test:
            index = self.asks.index(asks_test[0])
            order = self.asks[index]
            location = "asks"
        else:
            return False, False, False

        return order, index, location

    def receive(self):
        return json.loads(self.conn.recv())

    def manager(self):
        function_name = inspect.stack()[0][3]
        log.debug(function_name)  # Log function name upon execution

        log.debug("\nProcessing new order...\n")

        # order_type = None
        order = self.receive()
        if not order:
            return

        if "asks" in order:
            self.bids = order['bids']
            self.asks = order['asks']
            self._order_book = {"bids": self.bids,
                                "asks": self.asks}
            # order_type = "order_book"
        # log.info("\nFormatting bids and asks...\n")
        # self.__format_bids(order['bids'])
        # self.__format_asks(order['asks'])
        # order_type = "order_book"

        elif "create_update" in order:
            if order["create_update"]:
                self._add_order(order)
                # order_type = "create_order"

            elif order["delete_update"]:
                self._remove_order(order['delete_update']['order_id'])
                # order_type = "delete_order"

            elif order['trade_updates']:
                for trade in order["trade_updates"]:
                    self._add_trade(trade, order['timestamp'])
                    # order_type = "trade"

        # if order_type:
        #     return self._handle_subscriptions(order_type)
