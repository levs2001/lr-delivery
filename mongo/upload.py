from pymongo import MongoClient
import pandas as pd


class MongoRestaurant:
    def __init__(self, uri: str, db_name: str):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def upload(self, data_json_name, collection_name):
        df = pd.read_json(data_json_name)
        collection = self.db[collection_name]
        collection.insert_many(df.to_dict('records'))


if __name__ == '__main__':
    mongo = MongoRestaurant('mongodb://localhost:30001', 'restaurant_db')

    mongo.upload('clients.json', 'clients')
    mongo.upload('orders.json', 'orders')
    mongo.upload('restaurants.json', 'restaurants')
