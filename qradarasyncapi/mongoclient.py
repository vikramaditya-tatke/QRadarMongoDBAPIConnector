import os
import logging
import urllib.parse

import dotenv
from pathlib import Path
from pymongo import MongoClient

dotenv.load_dotenv(Path(__file__).parent.parent / "config" / ".env")


class MongoDBConnection(object):
    def __init__(self) -> None:
        self.host0 = os.environ.get("MONGO_HOST0")
        self.host1 = os.environ.get("MONGO_HOST1")
        self.host2 = os.environ.get("MONGO_HOST2")
        self.port = int(os.environ.get("MONGO_PORT"))
        self.username = os.environ.get("MONGO_USERNAME")
        self.pwd = urllib.parse.quote_plus(os.environ.get("MONGO_PWD"))

    def get_client(self) -> MongoClient:
        try:
            client: MongoClient = MongoClient(
                f"mongodb://{self.username}:{self.pwd}@{self.host0}:{self.port},{self.host1}:{self.port},{self.host2}:{self.port}/?authMechanism=SCRAM-SHA-1&authSource=admin"
            )
            return client
        except Exception as my_generic_exp:
            logging.warning(
                "Exception in MyMongoClient __init__(): %s", my_generic_exp
            )
