from config_file import *
from pymongo import MongoClient

# Replace with your MongoDB connection details
mongo_client = MongoClient(mongo_url)
db = mongo_client[database_name]
collection = db[collection_name]
