import logging
from pymongo import MongoClient
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# set the trading log
logging.basicConfig(
    filename='quant_trading.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MongoDBManager:
    def __init__(self, uri="mongodb://localhost:27017", db_name="quant_trading"): #connect my localhost path
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        logging.info(f"Connected to MongoDB at {uri}, database: {db_name}")
        self.create_indexes()

    def create_indexes(self):
        # create indexes to store the data fetching / ohlcv|orderbook|signals|positions|trades
        self.db.ohlcv.create_index(
            [("symbol", ASCENDING), ("timeframe", ASCENDING), ("timestamp", ASCENDING), ("exchange", ASCENDING)],
            unique=True
        )

        self.db.orderbook.create_index(
            [("symbol", ASCENDING), ("timestamp", ASCENDING), ("exchange", ASCENDING)],
            unique=True
        )

        self.db.trades.create_index(
            "order_id",
            unique=True,
            sparse=True  # allow null order_id
        )

        self.db.positions.create_index(
            [("symbol", ASCENDING), ("open_time", ASCENDING)]
        )

        self.db.signals.create_index(
            [("symbol", ASCENDING), ("timeframe", ASCENDING), ("timestamp", ASCENDING)]
        )
        logging.info("Indexes created successfully")

#insert data to index
    def insert_ohlcv(self, doc):
        try:
            self.db.ohlcv.insert_one(doc)
            logging.info(f"Inserted OHLCV document: {doc}")
        except DuplicateKeyError:
            logging.warning(f"Duplicate OHLCV: {doc}")

    def insert_orderbook(self, doc):
        try:
            self.db.orderbook.insert_one(doc)
            logging.info(f"Inserted Orderbook document: {doc}")
        except DuplicateKeyError:
            logging.warning(f"Duplicate Orderbook: {doc}")

    def insert_trade(self, doc):
        try:
            self.db.trades.insert_one(doc)
            logging.info(f"Inserted Trade document: {doc}")
        except DuplicateKeyError:
            logging.warning(f"Duplicate Trade: {doc}")

    def insert_position(self, doc):
        self.db.positions.insert_one(doc)
        logging.info(f"Inserted Position document: {doc}")

    def insert_signal(self, doc):
        self.db.signals.insert_one(doc)
        logging.info(f"Inserted Signal document: {doc}")

#close the database connection
    def close(self):
        self.client.close()
        logging.info("Closed MongoDB connection")