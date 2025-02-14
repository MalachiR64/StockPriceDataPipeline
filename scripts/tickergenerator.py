import json 
import csv
import yfinance as yf
import pandas as pd
from yahooquery import Ticker
from yahoo_fin.stock_info import tickers_sp500
#pip install yfinance[nospam]


#define the function
def populate_json_file ( json_file,list):
    with open(json_file, 'w') as file:
        json.dump(list, file, indent=4)

def populate_csv_file (csv_file, field_names):
    with open (csv_file, 'w', newline='')as file:
        writer = csv.DictWriter(file, fieldnames=field_names, delimiter=',')
        writer.writeheader()

def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    table = pd.read_html(url)[0]  # Get first table on the page
    return table["Symbol"].tolist()

def main():        
    # Step 1: initialize and populate the json files
    
    # Get all the tickers in S&P 500
    sp500_tickers = get_sp500_tickers()
    industries_dict = {"industries":{}}
    sectors_dict = {"sectors":{}}

    populate_json_file ('tickers.json',sp500_tickers)
    populate_json_file('industries.json',industries_dict)
    populate_json_file ('sectors.json',sectors_dict)

    # Step 2: initialize the csv files 
    stocks_field_names = ["stock_id","industry_id","sector_id","stock_symbol","stock_name","stock_price","market_cap", "last_updated"]
    industries_field_names = ["industry_id","sector_id","industry_name","market_cap", "last_updated"]
    sector_field_names = ["sector_id","sector_name","market_cap", "last_updated"]
    populate_csv_file("stocks.csv" , stocks_field_names)
    populate_csv_file("industries.csv" , industries_field_names)
    populate_csv_file("sectors.csv" , sector_field_names)
    
if __name__ == "__main__":
    main()