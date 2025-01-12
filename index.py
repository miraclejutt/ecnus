from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
from config import *

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Check if the database exists
if db_name not in client.list_database_names():
    # Create a new database
    db = client[db_name]

    # Create a new collection
    collection = db[db_collection]

    # Insert initial data and delete it to initialize the collection
    collection.insert_one({'test': 'test'})
    collection.delete_one({'test': 'test'})

    # Insert data from the CSV file
    df = pd.read_csv('Finalize Backend.csv')

    temp = df[['Heading', 'Content', 'Status for WZS', 'Official Status', 'Region', 
               'Priority Level', 'Type', 'Category', 'Platform']]

    # Drop all rows where 'Heading' or 'Content' is NaN
    temp = temp.dropna(subset=['Heading', 'Content'])

    # Add the data to the database
    collection.insert_many(temp.to_dict('records'))
    print("Inserted data from 'Finalize Backend.csv' to the database.")

    # create a new search index
    index = {
        "definition": {
            "mappings": {
                "dynamic": True
            }
        },
        "name": "rss-tag",
    }

    collection.create_search_index(index)
    print("Created search index rss-tag.")

else:
    print("Database already exists. Updating Index")

    # Access the database and collection
    db = client[db_name]
    collection = db[db_collection]

    new_index_definition = {
        "mappings": {
            "dynamic": True
        }
    }

    collection.update_search_index("rss-tag", new_index_definition)
    print("Updated search index rss-tag.")