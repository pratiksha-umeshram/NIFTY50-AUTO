import requests
import pandas as pd
import datetime
import yfinance as yf
import os
import json

def fetch_nifty50_data():
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers)
    response = session.get(url, headers=headers)
    data = response.json()

    records = []
    today = datetime.date.today().strftime("%Y-%m-%d")

    for stock in data["data"]:
        records.append({
            "Date": today,
            "Symbol": stock.get("symbol"),
            "Company": stock.get("identifier", "").replace("NSE:", ""),
            "Open": stock.get("open"),
            "DayHigh": stock.get("dayHigh"),
            "DayLow": stock.get("dayLow"),
            "LastPrice": stock.get("lastPrice"),
            "PreviousClose": stock.get("previousClose"),
            "Change": stock.get("change"),
            "PChange": stock.get("pChange"),
            "Volume": None,     # placeholder → will fill later
        })

    df = pd.DataFrame(records)
    print(f"Fetched {len(df)} stock records")
    return df


def enrich_volume(df):
    print("Fetching Volume from Yahoo Finance...")

    volumes = {}

    for symbol in df["Symbol"]:
        if symbol in ["NIFTY 50", "50"]:
            continue

        ticker = symbol + ".NS"

        try:
            data = yf.download(ticker, period="1d", interval="1d", progress=False)
            if len(data) > 0:
                volumes[symbol] = int(data["Volume"].iloc[-1])
        except:
            pass

    df["Volume"] = df["Symbol"].map(volumes)
    return df


def save_history(df):
    file_path = "nifty50_history.csv"

    if os.path.exists(file_path):
        old = pd.read_csv(file_path)
        df = pd.concat([old, df], ignore_index=True)
    else:
        df = df

    # 🔥 REMOVE DUPLICATES (MOST IMPORTANT)
    df.drop_duplicates(subset=["Date", "Symbol"], keep="last", inplace=True)

    # Save CSV
    df.to_csv(file_path, index=False)
    print(f"Saved to {file_path}")

    # Save JSON for Power BI cloud refresh
    df.to_json("nifty50.json", orient="records")
    print("Saved JSON for cloud access")

    return df


if __name__ == "__main__":
    df = fetch_nifty50_data()
    df = enrich_volume(df)
    save_history(df)
