# initialize mongo db client

from pymongo import MongoClient
from collections import Counter

# This file contains all the required configurations and paths

INTERVAL = 1 * 60 * 60  # 1 hours
LINKS = "links.txt"
RAW_DATA = "raw_csv"

CODA_TOKEN="6a34b485-5d04-42bd-813f-3d4f504e6436"
CODA_DOC="YaHDNs9ZKc"
CODA_TABLE="grid-cNd5sipcEc"

SHORT_IO_TOKEN="sk_iNQcNNiU7mWsw6eA"
SHORT_IO_DOMAIN="source.www.ecnus.com"

# MongoDB configurations
db_name = "rss"  # Database name
db_collection = "raw_data"  # Collection name

# MongoDB connection
client = MongoClient("mongodb+srv://ecnusofficial:PmyF9EUA86pmmI3q@automated-tagging.tzsbx.mongodb.net")  # Replace with your MongoDB connection string
db = client[db_name]  # Database name
collection = db[db_collection]  # Database collection
