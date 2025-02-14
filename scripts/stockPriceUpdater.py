import os, uuid
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from scripts.models import Base, Sector, Industry, Stock
import yfinance as yf
import pyodbc
import pandas as pd
import time
from scripts.uploadToAzureBlobAndSQL import files_blob_upload
def file_remover (removed_files):
    for file in removed_files:
        os.remove(file)
def file_downloader(downloaded_files, connect_str_blob,container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str_blob)
    container_client = blob_service_client.get_container_client(container_name)
    for file in downloaded_files:
        try:
            blob_client = container_client.get_blob_client(file)

            with open(file, "wb") as file:
                file.write(blob_client.download_blob().readall())
    
            print(f"Downloaded: {file}")   
        except Exception as e:
            print(f"Failed to download {file}: {e}")
def updated_stock_data(ticker):
    try: 
        formatted_ticker = ticker.replace(".", "-")  # Replace "." with "-"
        stock = yf.Ticker(formatted_ticker)
        info = stock.info
        time.sleep(1)
        return {
        "stock_price" : info.get('currentPrice', "NULL"),
        "stock_market_cap" : info.get('marketCap', "NULL")
        }
    except Exception as e:
        print(f"Error fetching data for {formatted_ticker}: {e}")
        return None 

def main():
    #input your connection strings
    connect_str_blob = ""
    container_name = "stockdatacsvs"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str_blob)    
    container_client = blob_service_client.get_container_client(container_name)
    # Open a database session
    connect_str_sql = ''
    conn = pyodbc.connect(connect_str_sql)
    cursor = conn.cursor()

    blob_files = ["stocks.csv","industries.csv","sectors.csv"]
    #download the files
    file_downloader(blob_files,connect_str_blob,container_name)

    #interate through the stocks
    df_stocks = pd.read_csv(blob_files[0])
    df_industries = pd.read_csv(blob_files[1])
    df_sectors = pd.read_csv(blob_files[2])

    #update the stock data
    update_stock_data = []
    for index, row in df_stocks.iterrows():
        new_stock_info = updated_stock_data(row.stock_symbol)
        new_stock_time = datetime.today().replace(microsecond=0)
        df_stocks.at[index, "stock_price"] = new_stock_info["stock_price"]
        df_stocks.at[index, "market_cap"] = new_stock_info["stock_market_cap"]
        df_stocks.at[index, "last_updated"] = new_stock_time
        #put it into the sql

        # Add SQL update query to batch
        update_stock_data.append(
        (
            new_stock_info["stock_price"],
            new_stock_info["stock_market_cap"],
            df_stocks.at[index, "last_updated"],
            row["stock_id"]
        )
        )
    #update the stock sql and csv file
    update_stock_query = """
        UPDATE stocks 
        SET price = ?, market_cap = ?, last_update = ?
        WHERE stock_id = ?
    """
    cursor.executemany(update_stock_query,update_stock_data)
    conn.commit()

    #write to stocks csv
    df_stocks.to_csv(blob_files[0], index=False)
    
    #update industries
    update_industry_data = []
    for index, row in df_industries.iterrows():
        new_industry_market_cap =int( df_stocks.loc[df_stocks['industry_id'] == row.industry_id, 'market_cap'].sum())
        new_industry_time = datetime.today().replace(microsecond=0)
        df_industries.at[index, "market_cap"] = new_industry_market_cap
        df_industries.at[index, "last_updated"] = new_industry_time
        update_industry_data.append((new_industry_market_cap,new_industry_time, row["industry_id"]))

    #update the industry sql and csv files
    update_industry_query = """
        UPDATE industries
        SET total_market_cap = ?, last_update = ?
        WHERE industry_id = ?
    """
    cursor.executemany(update_industry_query,update_industry_data)
    conn.commit()
    df_industries.to_csv(blob_files[1], index=False)

    #update sector
    update_sector_data = []
    for index, row in df_sectors.iterrows():
        new_sector_market_cap =int( df_industries.loc[df_industries['sector_id'] == row.sector_id, 'market_cap'].sum())
        new_sector_time = datetime.today().replace(microsecond=0)
        df_sectors.at[index, "market_cap"] = new_sector_market_cap
        df_sectors.at[index, "last_updated"] = new_sector_time
        update_sector_data.append((new_sector_market_cap,new_sector_time, row["sector_id"]))

    update_sector_query = """
        UPDATE sectors
        SET total_market_cap = ?, last_update = ?
        WHERE sector_id = ?
    """
    cursor.executemany(update_sector_query,update_sector_data)
    conn.commit()
    df_sectors.to_csv(blob_files[2], index=False)
    # Close SQL connection
    cursor.close()
    conn.close()

    files_blob_upload(blob_files,container_client)
    print("Stock data updated in Azure SQL and CSV successfully!")
    #remove files
    #file_remover(blob_files)
if __name__ == "__main__":
    main()