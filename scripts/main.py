from datetime import datetime
from scripts.tickergenerator import populate_json_file
import time
import json
import csv
from csv import writer
import os
import random
import yfinance as yf
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential 

def generate_unique_id_csv(csv_file, csv_file_id,):
    df = pd.read_csv(csv_file)
    used_ids = df[csv_file_id].tolist()
    while True:
        # Generate a random 5-digit ID
        new_id = random.randint(10000, 99999)
        
        # Check if the ID is already used
        if new_id not in used_ids:
            # If not, return the new ID
            return new_id

def fetch_stock_data(ticker):
    try: 
        formatted_ticker = ticker.replace(".", "-")  # Replace "." with "-"
        stock = yf.Ticker(formatted_ticker)
        info = stock.info
        stock_name = info.get('shortName', "NULL")
        stock_industry = info.get('industry', "NULL")
        stock_sector = info.get('sector', "NULL")
        time.sleep(1)
        stock_price = info.get('currentPrice', "NULL")
        stock_market_cap = info.get('marketCap', "NULL")
        time.sleep(1)
        return {
            "symbol": formatted_ticker,
            "name": stock_name,
            "industry": stock_industry,
            "sector": stock_sector,
            "price": stock_price,
            "market_cap": stock_market_cap,         
        }
    # you still need to delete this thing
    except Exception as e:
        print(f"Error fetching data for {formatted_ticker}: {e}")
        return None 

def append_to_csv(csv_file, row):
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(row)
 
def remove_blank_line(csv_file): 
    with open(csv_file, 'r') as file: 
        lines = file.readlines()
    if len(lines) > 1 and lines[1].strip() == '': 
        del lines[1] 
    with open(csv_file, 'w', newline='') as file: 
        file.writelines(lines)

#main function         
def main():
    # have access to all the files
    tickers_json = 'tickers.json'
    industries_json = 'industries.json'
    sectors_json = 'sectors.json'
    stocks_csv = "stocks.csv"
    sectors_csv = "sectors.csv"
    industries_csv = "industries.csv"
    # load remaining tickers to be iterated
    with open(tickers_json, 'r') as file:
        sp500_tickers = json.load(file) 
    #print(sp500_tickers)
    for ticker in sp500_tickers[:]:
        stock_industry_id = 0
        stock_sector_id = 0
        #making the api calls  
        stock_data = fetch_stock_data(ticker)
        # if there is a failure then stop the loop
        if not stock_data:
            break

        sp500_tickers.remove(ticker) #remove the ticker from ticker.json

        #finish the other attirubtes
        stock_last_update = datetime.today().replace(microsecond=0)
        stock_id = generate_unique_id_csv(stocks_csv, "stock_id")

        #set data from the api call 
        stock_industry = stock_data["industry"]
        stock_sector = stock_data["sector"]
        symbol = stock_data["symbol"]
        stock_name = stock_data["name"]
        stock_price = stock_data["price"]
        stock_market_cap = stock_data["market_cap"]

        #get the industry_id
        with open (industries_json, 'r') as json_file:
            industry_dict = json.load(json_file)
        if stock_industry not in industry_dict["industries"]:
            stock_industry_id = (generate_unique_id_csv(industries_csv, "industry_id"))
            industry_dict["industries"][stock_industry]= {
                "industry_id": stock_industry_id,
                "sector" : stock_sector
            }
        else:
            stock_industry_id = industry_dict["industries"][stock_industry]["industry_id"] 
        with open (industries_json, 'w') as json_file:
            json.dump(industry_dict ,json_file, indent = 4)


        #get the sector_id
        with open (sectors_json, 'r') as json_file:
            sectors_dict = json.load(json_file)
        if stock_sector not in sectors_dict["sectors"]:
            stock_sector_id = (generate_unique_id_csv(sectors_csv, "sector_id"))
            sectors_dict["sectors"][stock_sector]= {
                "sector_id": stock_sector_id
            }
        else:
            stock_sector_id = sectors_dict["sectors"][stock_sector]["sector_id"]
        with open (sectors_json, 'w') as json_file:
            json.dump(sectors_dict,json_file, indent = 4)

        #append to stock_csv
        stock_list = [stock_id,stock_industry_id,stock_sector_id,symbol,stock_name,stock_price,stock_market_cap,stock_last_update]
        append_to_csv(stocks_csv,stock_list)


    #the api calls are done
    populate_json_file(tickers_json,sp500_tickers)

    #if there are still stocks not in csv run the script again
    if sp500_tickers != []:
        exit()
    else:
        os.remove("tickers.json")

    #populate the other csvs
    with open (industries_json, 'r') as json_file:
        industry_dict = json.load(json_file)
    with open (sectors_json, 'r') as json_file:
        sectors_dict = json.load(json_file)
    df_stocks = pd.read_csv('stocks.csv')

    #poputlate the industry csv
    for industry in industry_dict["industries"]:
        industry_id = industry_dict["industries"][industry]["industry_id"]
        industry_sector = industry_dict["industries"][industry]["sector"]
        industry_sector_id = sectors_dict["sectors"][industry_sector]["sector_id"]
        industry_total_market_cap = df_stocks.loc[df_stocks['industry_id'] == industry_id, 'market_cap'].sum()
        industry_last_update = datetime.today().replace(microsecond=0)
        industry_list = [industry_id, industry_sector_id, industry, industry_total_market_cap,industry_last_update]
        append_to_csv(industries_csv,industry_list)    

    #populate the sector csv 
    df_industries = pd.read_csv('industries.csv')
    for sector in sectors_dict["sectors"]:
        sector_id = sectors_dict["sectors"][sector]["sector_id"]
        sector_total_market_cap = df_industries.loc[df_industries["sector_id"]== sector_id, 'market_cap'].sum()
        sector_last_update = datetime.today().replace(microsecond=0)
        sector_list = [sector_id,sector,sector_total_market_cap, sector_last_update]
        append_to_csv(sectors_csv, sector_list)

    # Clean up blank lines
    for file in [stocks_csv, industries_csv, sectors_csv]:
        remove_blank_line(file)

if __name__ == "__main__":
    main()