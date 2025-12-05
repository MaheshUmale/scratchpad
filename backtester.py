import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import sys
import time
from collections import deque

# Assuming ORDER_FLOW_s9 contains the necessary classes and functions
# We will need to refactor it to be importable
from ORDER_FLOW_s9 import StrategyEngine, PaperTradeManager, DataPersistor

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "upstox_strategy_db"
TICK_COLLECTION = "tick_data"
BACKTEST_SIGNAL_COLLECTION = "backtest_signals"

def run_backtest(start_time, end_time):
    """
    Runs a backtest of the trading strategy on historical tick data.
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    tick_collection = db[TICK_COLLECTION]

    # Use a separate collection for backtest signals
    persistor = DataPersistor()
    persistor.db = db
    persistor.SIGNAL_COLLECTION = BACKTEST_SIGNAL_COLLECTION

    trade_manager = PaperTradeManager(persistor=persistor)
    strategy_engine = StrategyEngine(persistor=persistor, trade_manager=trade_manager)

    # Fetch historical data
    ticks = tick_collection.find({
        "_insertion_time": {
            "$gte": start_time,
            "$lte": end_time
        }
    }).sort("_insertion_time", 1)

    for tick in ticks:
        ltpc_data = tick.get('fullFeed', {}).get('marketFF', {}).get('ltpc', {})
        ltq = ltpc_data.get('ltq', 0)

        try:
            ltq = int(ltq)
        except (ValueError, TypeError):
            ltq = 0

        strategy_engine.process_tick(tick, ltq)

    print("Backtest complete.")
    # In a real scenario, you'd generate a more detailed report here
    generate_backtest_report(db, BACKTEST_SIGNAL_COLLECTION)

def generate_backtest_report(db, collection_name):
    """
    Generates a performance report from the backtest signals.
    """
    signals = pd.DataFrame(list(db[collection_name].find()))
    if signals.empty:
        print("No signals were generated during the backtest.")
        return

    # PnL Analysis
    pnl = signals[signals['signal'] == 'SQUARE_OFF']['pnl'].sum()
    print(f"Total PnL: {pnl}")

    # Further analysis can be added here (e.g., win/loss ratio, Sharpe ratio, etc.)

if __name__ == "__main__":
    # Example: Run backtest for the last day
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)

    print(f"Running backtest from {start_time} to {end_time}...")
    run_backtest(start_time, end_time)
