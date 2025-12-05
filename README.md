# Professional Options Trading Platform

This project is a professional options trading platform with a focus on NIFTY and BANK NIFTY options trading. It utilizes the Upstox API to fetch and analyze option chain data, providing insights into market trends and potential trading opportunities.

## Features

- **Option Chain Analysis:** The platform fetches option chain data and calculates various metrics, such as change in Open Interest (OI), to identify market trends like long buildup, short covering, long unwinding, and short buildup.
- **Trading Strategy:** The core trading logic is based on an Order Book Imbalance (OBI) and High Volume Node (HVN) strategy, augmented with support and resistance levels derived from OI concentration.
- **Web-based UI:** A Flask-based web application provides a user-friendly interface to visualize the option chain, OI metrics, and individual trades on a chart.
- **Backtesting:** The platform includes a backtesting feature that allows for testing the trading strategy on historical tick data.
- **Real-time Data:** The application uses WebSockets to receive a live feed of market data from Upstox.

## How to Run

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Set Up MongoDB:** Make sure you have a running instance of MongoDB.
3.  **Configure API Credentials:** Set your Upstox API access token as an environment variable:
    ```bash
    export UPSTOX_ACCESS_TOKEN='your_access_token'
    ```
4.  **Run the Trading Application:**
    ```bash
    python ORDER_FLOW_s9.py
    ```
5.  **Run the Flask UI:**
    ```bash
    python ORDER_FLOW_FLASK2.py
    ```
6.  **Run the Backtester (Optional):**
    ```bash
    python backtester.py
    ```
