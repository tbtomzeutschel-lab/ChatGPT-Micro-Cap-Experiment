import os
import requests
import csv

# Pfad zur Microcap-Liste (aus "Start Your Own")
MICROCAP_CSV = "Start Your Own/microcap_universe.csv"

# API Key wird in GitHub Secrets gespeichert
API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")

BASE_URL = "https://www.alphavantage.co/query"
DATA_DIR = "data/prices"


def load_microcap_tickers(csv_path):
    """
    Lädt Micro-Cap-Ticker aus der CSV-Datei und filtert nach:
    - region in {US, EU}
    - market_cap_musd < 300
    - active == 1
    """
    tickers = []

    if not os.path.isfile(csv_path):
        print(f"⚠️ CSV-Datei {csv_path} nicht gefunden. Keine Ticker geladen.")
        return tickers

    with open(csv_path, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                region = row.get("region", "").strip().upper()
                market_cap = float(row.get("market_cap_musd", "0") or 0)
                active = row.get("active", "0").strip()

                if region not in {"US", "EU"}:
                    continue
                if market_cap >= 300:
                    continue
                if active not in {"1", "true", "True"}:
                    continue

                ticker = row.get("ticker", "").strip()
                if ticker:
                    tickers.append(ticker)

            except Exception as e:
                print(f"⚠️ Fehler beim Verarbeiten einer Zeile: {e}")

    print(f"✅ {len(tickers)} aktive Micro-Cap-Ticker geladen: {tickers}")
    return tickers


def fetch_daily_time_series(ticker):
    """Holt tägliche Kursdaten für einen Ticker von Alpha Vantage."""
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "apikey": API_KEY,
        "outputsize": "compact",
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    time_series = data.get("Time Series (Daily)", {})

    if not time_series:
        print(f"⚠️ Keine Daten für {ticker} gefunden.")
        return []

    rows = []
    for date_str, values in sorted(time_series.items()):
        row = {
            "date": date_str,
            "ticker": ticker,
            "open": values["1. open"],
            "high": values["2. high"],
            "low": values["3. low"],
            "close": values["4. close"],
            "volume": values["6. volume"],
        }
        rows.append(row)

    return rows


def append_to_csv(ticker, rows):
    """Hängt neue Tagesdaten an die CSV-Datei an (ohne Duplikate)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, f"{ticker}.csv")

    file_exists = os.path.isfile(filepath)

    # vorhandene Dates sammeln, um doppelte Einträge zu vermeiden
    existing_dates = set()
    if file_exists:
        with open(filepath, mode="r") as readfile:
            reader = csv.DictReader(readfile)
            for row in reader:
                existing_dates.add(row["date"])

    # Daten anhängen
    with open(filepath, mode="a", newline="") as csvfile:
        fieldnames = ["date", "ticker", "open", "high", "low", "close", "volume"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for row in rows:
            if row["date"] not in existing_dates:
                writer.writerow(row)


def main():
    if not API_KEY:
        raise ValueError("❌ Fehler: Kein API-Key gesetzt! Setze ALPHAVANTAGE_API_KEY in GitHub Secrets.")

    tickers = load_microcap_tickers(MICROCAP_CSV)
    if not tickers:
        print("⚠️ Keine Ticker geladen. Beende Lauf.")
        return

    for ticker in tickers:
        print(f"⬇️ Hole Daten für {ticker} ...")
        rows = fetch_daily_time_series(ticker)

        if rows:
            append_to_csv(ticker, rows)
            print(f"✅ Daten aktualisiert: {ticker}.csv")
        else:
            print(f"⚠️ Keine Daten für {ticker}.")


if __name__ == "__main__":
    main()
