import upstox_client
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

# --- Configuration ---
ACCESS_TOKEN = os.environ.get('UPSTOX_ACCESS_TOKEN', 'YOUR_DEFAULT_TOKEN')

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "upstox_strategy_db"
OC_COLLECTION_NAME = "option_chain"

def get_api_client():
    """Initializes and returns an Upstox API client instance."""
    configuration = upstox_client.Configuration()
    configuration.access_token = ACCESS_TOKEN
    api_client = upstox_client.ApiClient(configuration)
    return api_client

def get_option_contracts(api_client, instrument_key):
    """
    Fetches the option contracts for a given instrument key.
    """
    try:
        api_instance = upstox_client.MarketQuoteApi(api_client)
        api_response = api_instance.get_option_contract(instrument_key)
        return api_response.data
    except upstox_client.ApiException as e:
        print(f"Error fetching option contracts: {e}")
        return None

def get_option_chain(api_client, instrument_key, expiry_date):
    """
    Fetches the option chain for a given instrument key.

    Args:
        api_client: An instance of the Upstox API client.
        instrument_key: The instrument key for the underlying (e.g., 'NSE_INDEX|Nifty 50').
        expiry_date: The expiry date of the option chain in 'YYYY-MM-DD' format.

    Returns:
        A dictionary containing the option chain data, or None if an error occurs.
    """
    try:
        api_instance = upstox_client.MarketQuoteApi(api_client)
        api_response = api_instance.get_put_call_option_chain(instrument_key, expiry_date)
        return api_response.data
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
    merged_df = pd.merge(df, prev_df, on="strike_price", suffixes=("", "_prev"), how='left')

    # Calculate Change in OI
    merged_df['ce_oi_change'] = (merged_df['ce_open_interest'] - merged_df['ce_open_interest_prev']).fillna(0)
    merged_df['pe_oi_change'] = (merged_df['pe_open_interest'] - merged_df['pe_open_interest_prev']).fillna(0)

    # Identify Buildups and Unwinding
    merged_df['ce_long_buildup'] = (merged_df['ce_oi_change'] > 0) & (merged_df['ce_ltp'] > merged_df['ce_ltp_prev'])
    merged_df['ce_short_buildup'] = (merged_df['ce_oi_change'] > 0) & (merged_df['ce_ltp'] < merged_df['ce_ltp_prev'])
    merged_df['ce_long_unwinding'] = (merged_df['ce_oi_change'] < 0) & (merged_df['ce_ltp'] < merged_df['ce_ltp_prev'])
    merged_df['ce_short_covering'] = (merged_df['ce_oi_change'] < 0) & (merged_df['ce_ltp'] > merged_df['ce_ltp_prev'])

    merged_df['pe_long_buildup'] = (merged_df['pe_oi_change'] > 0) & (merged_df['pe_ltp'] > merged_df['pe_ltp_prev'])
    merged_df['pe_short_buildup'] = (merged_df['pe_oi_change'] > 0) & (merged_df['pe_ltp'] < merged_df['pe_ltp_prev'])
    merged_df['pe_long_unwinding'] = (merged_df['pe_oi_change'] < 0) & (merged_df['pe_ltp'] < merged_df['pe_ltp_prev'])
    merged_df['pe_short_covering'] = (merged_df['pe_oi_change'] < 0) & (merged_df['pe_ltp'] > merged_df['pe_ltp_prev'])

    return merged_df
