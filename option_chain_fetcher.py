import upstox_client
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

# --- Configuration ---
API_VERSION = "v2"
ACCESS_TOKEN = os.environ.get('UPSTOX_ACCESS_TOKEN', 'YOUR_DEFAULT_TOKEN')

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "upstox_strategy_db"
OC_COLLECTION_NAME = "option_chain"

def get_api_client(api_version):
    """Initializes and returns an Upstox API client instance."""
    client = upstox_client.ApiClient(api_version)
    client.set_access_token(ACCESS_TOKEN)
    return client

def get_option_chain(api_client, instrument_key):
    """
    Fetches the option chain for a given instrument key.

    Args:
        api_client: An instance of the Upstox API client.
        instrument_key: The instrument key for the underlying (e.g., 'NSE_INDEX|Nifty 50').

    Returns:
        A dictionary containing the option chain data, or None if an error occurs.
    """
    try:
        opts_api = upstox_client.OptionsApi(api_client)
        response = opts_api.get_option_chain(instrument_key=instrument_key)
        return response.data
    except upstox_client.ApiException as e:
        print(f"Error fetching option chain: {e}")
        return None

def store_option_chain_data(data):
    """
    Stores the fetched option chain data in a MongoDB collection.

    Args:
        data: The option chain data to be stored.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        collection = db[OC_COLLECTION_NAME]

        # Get previous data for OI calculation
        prev_data = collection.find_one(
            {"instrument_key": data['instrument_key']},
            sort=[("timestamp", -1)]
        )

        df = pd.DataFrame(data['options_chain'])
        if prev_data:
            prev_df = pd.DataFrame(prev_data['options_chain'])
            df = calculate_oi_metrics(df, prev_df)

        data['options_chain'] = df.to_dict('records')

        # Add a timestamp to the data
        data['timestamp'] = datetime.now()

        collection.insert_one(data)
        print("Successfully stored option chain data in MongoDB.")
    except Exception as e:
        print(f"Error storing option chain data in MongoDB: {e}")

def calculate_oi_metrics(df, prev_df):
    """
    Calculates various OI-based metrics from the option chain DataFrame.

    Args:
        df: A pandas DataFrame containing the current option chain data.
        prev_df: A pandas DataFrame containing the previous option chain data.

    Returns:
        A dictionary containing the calculated metrics.
    """
    merged_df = pd.merge(df, prev_df, on="strike_price", suffixes=("", "_prev"))

    # Calculate Change in OI
    df['ce_oi_change'] = merged_df['ce_open_interest'] - merged_df['ce_open_interest_prev']
    df['pe_oi_change'] = merged_df['pe_open_interest'] - merged_df['pe_open_interest_prev']

    # Identify Buildups and Unwinding
    df['ce_long_buildup'] = (df['ce_oi_change'] > 0) & (df['ce_ltp'].diff() > 0)
    df['ce_short_buildup'] = (df['ce_oi_change'] > 0) & (df['ce_ltp'].diff() < 0)
    df['ce_long_unwinding'] = (df['ce_oi_change'] < 0) & (df['ce_ltp'].diff() < 0)
    df['ce_short_covering'] = (df['ce_oi_change'] < 0) & (df['ce_ltp'].diff() > 0)

    df['pe_long_buildup'] = (df['pe_oi_change'] > 0) & (df['pe_ltp'].diff() > 0)
    df['pe_short_buildup'] = (df['pe_oi_change'] > 0) & (df['pe_ltp'].diff() < 0)
    df['pe_long_unwinding'] = (df['pe_oi_change'] < 0) & (df['pe_ltp'].diff() < 0)
    df['pe_short_covering'] = (df['pe_oi_change'] < 0) & (df['pe_ltp'].diff() > 0)

    return df

def main():
    """
    Main function to fetch, process, and store option chain data.
    """
    api_client = get_api_client(API_VERSION)

    # Example for NIFTY
    nifty_key = "NSE_INDEX|Nifty 50"
    option_chain_data = get_option_chain(api_client, nifty_key)

    if option_chain_data:
        store_option_chain_data(option_chain_data)

        # Convert to pandas DataFrame for analysis
        df = pd.DataFrame(option_chain_data['options_chain'])
        metrics_df = calculate_oi_metrics(df)

        print("\nOption Chain Metrics:")
        print(metrics_df[['strike_price', 'ce_long_buildup', 'ce_short_buildup', 'pe_long_buildup', 'pe_short_buildup']].head())

if __name__ == "__main__":
    main()
