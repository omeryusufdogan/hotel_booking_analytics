import yfinance as yf
import pandas as pd

def main():
    print("Fetching exchange rates from API, please wait...")
    
    # Currency pairs and Yahoo Finance tickers
    currency_pairs = {
        'EUR': 'EURUSD=X',
        'INR': 'INRUSD=X'
    }
    
    all_rates = []
    
    for currency, ticker in currency_pairs.items():
        # Download historical data
        data = yf.download(ticker, start="2024-01-01", end="2026-12-31")
        df = data.reset_index()
        
        # Flatten columns if MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        # Keep only Date and Close columns
        df = df[['Date', 'Close']].copy()
        
        # Rename columns and add base currency
        df.columns = ['DATE', 'USD_RATE']
        df['BASE_CURRENCY'] = currency
        
        all_rates.append(df)
        
    # Combine all currency dataframes and forward-fill missing values
    final_rates = pd.concat(all_rates, ignore_index=True)
    final_rates['DATE'] = pd.to_datetime(final_rates['DATE']).dt.date
    final_rates['USD_RATE'] = final_rates['USD_RATE'].ffill()
    
    # Save CSV to specified path
    output_path = r"C:\Users\omery\OneDrive\Desktop\exchange_rates.csv"
    final_rates.to_csv(output_path, index=False)
    
    print(f"Done! CSV saved to: {output_path}")

if __name__ == "__main__":
    main()
