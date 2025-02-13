import yfinance as yf
import pandas as pd
import duckdb
from datetime import datetime
# top 15 companies
sensex_stocks = {
    "BAJAJFINSV.NS": "Bajaj Finserv",
    "HDFCBANK.NS": "HDFC Bank",
    "RELIANCE.NS": "Reliance Industries",
    "ASIANPAINT.NS": "Asian Paints",
    "SBIN.NS": "State Bank of India",
    "TATAMOTORS.NS": "Tata Motors",
    "MARUTI.NS": "Maruti Suzuki",
    "ZOMATO.NS": "Zomato",
    "ICICIBANK.NS": "ICICI Bank",
    "INFY.NS": "Infosys",
    "M&M.NS": "Mahindra & Mahindra",
    "ITC.NS": "ITC Limited",
    "LT.NS": "Larsen & Toubro",
    "BHARTIARTL.NS": "Bharti Airtel",
    "ADANIPORTS.NS": "Adani Ports & SEZ"
}
today = datetime.today().strftime('%Y-%m-%d') # to fetch today date
sensex_data = []  # to store sensex data
closing_data = [] # to store closing price data
for symbol, name in sensex_stocks.items():      #today closing price of stock
    stock = yf.Ticker(symbol)                   # symbol of stock
    df = stock.history(period="1d")
    sensex_data.append([symbol, name])
    closing_price = df["Close"].iloc[0] if not df.empty else None
    active_status = "Yes" if closing_price is not None else "No"
    closing_data.append([today, symbol, closing_price, active_status])
df_sensex = pd.DataFrame(sensex_data, columns=["Symbol", "Stock_Name"])    # converting to data frame
df_closing = pd.DataFrame(closing_data, columns=["Date", "Symbol", "Closing_Price", "Active"])
# Connecting to DuckDB
con = duckdb.connect("sensex_data.db")
con.execute("DROP TABLE IF EXISTS closing_prices;") #drop tables if any old tables exist
con.execute("DROP TABLE IF EXISTS sensex_stocks;")
# Sensex table
con.execute("""
    CREATE TABLE sensex_stocks (
        Symbol TEXT PRIMARY KEY,
        Stock_Name TEXT);
""")
# closing price table
con.execute("""
    CREATE TABLE closing_prices (
        Date TEXT,
        Symbol TEXT REFERENCES sensex_stocks(Symbol),  -- Foreign Key
        Closing_Price FLOAT,
        Active TEXT);
""")
# Insert data into tables
con.register("temp_sensex", df_sensex)
con.execute("INSERT INTO sensex_stocks SELECT * FROM temp_sensex")
con.register("temp_closing", df_closing)
con.execute("INSERT INTO closing_prices SELECT * FROM temp_closing")
# Close connection
con.close()
print("Today's data saved successfully into DuckDB!")

#to fetch sensex table
import duckdb
con = duckdb.connect("sensex_data.db")
df = con.execute("SELECT * FROM sensex_stocks").fetchdf()
con.close()
print(df)

#to fetch closing price table
import duckdb
con = duckdb.connect("sensex_data.db")
df = con.execute("SELECT * FROM closing_prices").fetchdf()
con.close()
print(df)
