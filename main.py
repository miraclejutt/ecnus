import warnings
import os
import requests
import pandas as pd
import numpy as np
import time
import logging
from config import *
from utils import *

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def download_data():
    """
    Downloads data from the links specified in the LINKS file and saves them as CSV files in the RAW_DATA folder.
    If the folder does not exist, it creates it. If it exists, it clears all existing files before downloading new ones.
    """
    try:
        if not os.path.exists(RAW_DATA):
            os.makedirs(RAW_DATA)
            logging.info(f"Created directory: {RAW_DATA}")
        else:
            for file in os.listdir(RAW_DATA):
                os.remove(f"{RAW_DATA}/{file}")
            logging.info(f"Cleared existing files in directory: {RAW_DATA}")

        with open(LINKS, "r") as f:
            links = f.readlines()

        for link in links:
            link = f"{link.strip().replace('feed','feeds')}.csv"
            response = requests.get(link)

            if response.status_code == 200:
                file_name = (
                    f"{RAW_DATA}/{str(response.content).split(',')[15][1:-1].replace('/','|')}.csv"
                )
                with open(file_name, "wb") as f:
                    f.write(response.content)
                logging.info(f"Downloaded and saved file: {file_name}")
            else:
                logging.error(f"Failed to download data from: {link}. Status code: {response.status_code}")

        logging.info("All files downloaded successfully.")

    except Exception as e:
        logging.exception(f"An error occurred in download_data: {e}")

def get_last_interval(file_path):
    """
    Reads a CSV file and filters data to include only the last 60 minutes based on the 'Date' column.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Filtered dataframe with the last 60 minutes of data.
    """
    try:
        data = pd.read_csv(file_path)
        data["unix_timestamp"] = pd.to_datetime(data["Date"]).astype(int) // 10**9
        current_unix_timestamp = int(time.time())
        return data[data["unix_timestamp"] > current_unix_timestamp - INTERVAL]
    except Exception as e:
        logging.exception(f"An error occurred while processing file {file_path}: {e}")
        return pd.DataFrame()

def process_data():
    """
    Combines data from all CSV files in RAW_DATA folder, filters it to include only the last 60 minutes of data,
    and saves the result as a single CSV file named 'interval_data.csv'.
    """
    try:
        data = pd.DataFrame()
        for file in os.listdir(RAW_DATA):
            file_path = f"{RAW_DATA}/{file}"
            data = pd.concat([data, get_last_interval(file_path)], ignore_index=True)

        data.to_csv("interval_data.csv", index=False)
        logging.info("Data for the interval saved successfully in 'interval_data.csv'.")

    except Exception as e:
        logging.exception(f"An error occurred in process_data: {e}")

def tag_data():
    """
    Tags the data in 'interval_data.csv' using a prediction function, merges the results with the input data,
    and saves the final output as 'final.csv'.
    """
    try:
        df = pd.read_csv("interval_data.csv")
        temp = df[['Title', 'Link', 'Plain Description', 'Author', 'Date']]

        # copy Title to Plain Description if Plain Description is NaN
        temp['Plain Description'] = np.where(temp['Plain Description'].isnull(), temp['Title'], temp['Plain Description'])

        temp['Date'] = pd.to_datetime(temp['Date']).dt.strftime('%m/%d/%Y')

        output = []
        for i in range(len(temp)):
            query_text = temp['Plain Description'][i]
            try:
                prediction = get_prediction(query_text, client, db, collection)
                output.append(prediction)
            except Exception as e:
                logging.error(f"Error predicting for row {i}")
                output.append({})

        output_df = pd.DataFrame(output)
        final = pd.concat([temp, output_df], axis=1)

        # set official status to 'Final Check'
        final['Official Status'] = 'Final Check'

        # Set Attachments to ''
        final['Attachments'] = ''

        # set Source Button, Publish FOR WZS, Archive to model
        final['Source Button'] = 'model'
        final['Publish FOR WZS'] = 'model'
        final['Archive'] = 'model'

        # rename column Link to Live Source
        final.rename(columns={'Link': 'Live Source'}, inplace=True)

        # rename Title to Heading
        final.rename(columns={'Title': 'Heading'}, inplace=True)

        # reanme Plain Description to Content
        final.rename(columns={'Plain Description': 'Content'}, inplace=True)

        # reanme data to Published Date
        final.rename(columns={'Date': 'Published Date'}, inplace=True)

        # add a column for Original Source equal to Live Source
        final['Original Source'] = final['Live Source']

        # apply link shortening on Live Source
        final['Live Source'] = final['Live Source'].apply(lambda url: shorten_url(url))

        # Change Status for WZS to '' if not Yes
        final['Status for WZS'] = np.where(final['Status for WZS'] != 'Yes', '', final['Status for WZS'])

        final.to_csv('interval_data.csv', index=False)

        # append to Finalize Backend.csv
        df = pd.read_csv('Finalize Backend.csv')
        df = pd.concat([df, final], ignore_index=True)
        df.to_csv('Finalize Backend.csv', index=False)
        logging.info("Tagged data saved successfully in 'Finalize Backend.csv'.")

        # add to the database
        try:
            df = pd.read_csv('interval_data.csv')

            temp = df[['Heading', 'Content', 'Status for WZS', 'Official Status', 'Region', 
                    'Priority Level', 'Type', 'Category', 'Platform']]
            temp = temp.dropna(subset=['Heading', 'Content'])
            collection.insert_many(temp.to_dict('records'))
            logging.info("Tagged data saved successfully in the database.")
        except Exception as e:
            logging.error(f"Error saving to database: {e}")

        logging.info("Tagged data saved successfully in 'interval_data.csv'.")

    except Exception as e:
        logging.exception(f"An error occurred in tag_data: {e}")

def send_to_coda():
    # Example usage:
    try:
        # Load the dataframe
        df = pd.read_csv('interval_data.csv')
        logging.info("CSV file 'interval_data.csv' loaded successfully.")
        
        # Insert dataframe to Coda
        insert_dataframe_to_coda(df, CODA_TOKEN, CODA_DOC, CODA_TABLE)
    except FileNotFoundError as file_err:
        logging.error("File not found: %s", file_err)
    except pd.errors.EmptyDataError as empty_err:
        logging.error("No data: %s", empty_err)
    except Exception as e:
        logging.error("An unexpected error occurred while processing the CSV file: %s", e)

def main():
    """
    Main function to orchestrate the pipeline for downloading, processing, and tagging data.
    """
    try:
        logging.info("Pipeline started.")
        download_data()
        process_data()
        tag_data()
        send_to_coda()
        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.exception(f"An error occurred in the pipeline: {e}")

if __name__ == "__main__":
    main()
