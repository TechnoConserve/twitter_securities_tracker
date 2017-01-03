from ib.opt import ibConnection


class PriceGrabber(object):
    price = {}

    def __init__(self):
        self.tws = ibConnection('localhost', 4002, 0)
        self.tws.register(self.tick_price_handler, 'TickPrice')
        self.tws.register(self.error_handler, 'Error')
        self.tws.connect()
        self._reqId = 1

    def error_handler(self, msg):
        print(msg)

    def tick_price_handler(self, msg):
        if msg.field == 4:
            try:
                # Now we can access the price with the symbol as the key
                symbol_key = self.price.pop(msg.tickerId)
                self.price[symbol_key] = msg.price
                # Stop the datafeed so we don't hit the ticker limit
                self.tws.cancelMktData(msg.tickerId)
            except KeyError:
                return

    def request_data(self, contract):
        self.tws.reqMktData(self._reqId, contract, '', False)
        # Save the request ID and match it to the symbol
        self.price[self._reqId] = contract.m_symbol
        self._reqId += 1
