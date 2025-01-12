import re
import logging
import requests
import pandas as pd
from config import *
from collections import Counter
from nltk import download
from nltk.corpus import stopwords

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Download stopwords
download("stopwords")
stop_words = set(stopwords.words("english"))

def clean_text(text, top_n=1000):
    """
    Clean the input text and return the top N unique words based on frequency.

    Args:
        text (str): Input text to clean.
        top_n (int): Number of unique words to return (default is 1000).

    Returns:
        list: Top N unique words sorted by frequency.
    """
    try:
        # Convert to lowercase
        text = str(text).lower()

        # Replace non-alphabetic characters with space
        text = re.sub(r"[^a-zA-Z]+", " ", text)

        # Remove URLs
        text = re.sub(r"http\S+", "", text)

        # Remove HTML tags
        text = re.sub(r"<.*?>", "", text)

        # Remove punctuations
        punctuations = "@#!?+&*[]-%.:/();$=><|{}^'`_"
        for p in punctuations:
            text = text.replace(p, "")

        # Remove emojis
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # Emoticons
            "\U0001F300-\U0001F5FF"  # Symbols & pictographs
            "\U0001F680-\U0001F6FF"  # Transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # Flags (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE,
        )
        text = emoji_pattern.sub(r"", text)

        # Tokenize, remove stopwords, and filter empty tokens
        words = [word for word in text.split() if word not in stop_words and word]

        # Count word frequencies
        word_counts = Counter(words)

        # Get the top N unique words
        top_words = [word for word, _ in word_counts.most_common(top_n)]

        return top_words

    except Exception as e:
        logging.exception(f"Error in clean_text: {e}")
        return []

def get_prediction(query_text, client, db, collection):
    """
    Perform a MongoDB search using an aggregation pipeline based on the cleaned query text.

    Args:
        query_text (str): The input query text to search.
        client: MongoDB client instance.
        db: MongoDB database instance.
        collection: MongoDB collection instance.

    Returns:
        dict: Dictionary containing fields and their most common values from the results.
    """
    try:
        # Clean the query text and limit to top 800 words
        query_text = clean_text(str(query_text), top_n=800)

        # MongoDB aggregation pipeline
        pipeline = [
            {
                "$search": {
                    "index": "rss-tag",
                    "text": {
                        "query": query_text,
                        "path": {"wildcard": "*"}
                    }
                }
            },
            {"$limit": 1},  # Get top N results
            {
                "$project": {
                    "Official Status": 1,
                    "Platform": 1,
                    "Priority Level": 1,
                    "Region": 1,
                    "Status for WZS": 1,
                    "Type": 1,
                    "Category": 1
                }
            }
        ]

        # Execute query
        results = list(collection.aggregate(pipeline))

        # Extract fields and compute mode
        fields = ["Official Status", "Platform", "Priority Level", "Region", "Status for WZS", "Type", "Category"]
        output = {}

        for field in fields:
            values = [doc.get(field, None) for doc in results if doc.get(field) is not None]
            mode_value = Counter(values).most_common(1)
            output[field] = mode_value[0][0] if mode_value else ""  # Get mode or empty string if not found

        return output

    except Exception as e:
        logging.exception(f"Error in get_prediction")
        return {}


# Define your API parameters
SHORTEN_URL_API = 'https://api.short.io/links'
HEADERS = {
    'authorization': SHORT_IO_TOKEN,
    'content-type': 'application/json'
}
DOMAIN = SHORT_IO_DOMAIN

# Lambda function to shorten URLs
def shorten_url(long_url):
    try:
        response = requests.post(
            SHORTEN_URL_API,
            json={
                'domain': DOMAIN,
                'originalURL': long_url,
            },
            headers=HEADERS
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json().get('shortURL')  # Extract and return the shortened URL
    except Exception as e:
        print(f"Error shortening URL {long_url}: {e}")
        return None  # Return None if the request fails