import os, uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from scripts.models import Base, Sector, Industry, Stock
import pyodbc
import pandas as pd


def files_blob_upload(files_to_upload,container_client):
    for file in files_to_upload:
        try:
            # Upload file
            with open(file, "rb") as data:
                container_client.upload_blob(name=file, data=data, overwrite=True)
            print(f"Uploaded {file} successfully.")
        except Exception as e:
            print(f"Failed to upload {file}: {e}")


def main():
    #connection string method
    ## Azure storage account details
    connect_str_blob = ""
    container_name = "stockdatacsvs"


    connect_str_sql = ''
    conn = pyodbc.connect(connect_str_sql)
    cursor = conn.cursor()

    def load_csv_to_sql(csv_file, table_name, conn):
        df = pd.read_csv(csv_file, encoding='utf-8')
        if (csv_file == 'stocks.csv'):
            for _, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {table_name} 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', tuple(row))
        elif (csv_file == 'industries.csv'):
            for _, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {table_name} 
                VALUES (?, ?, ?, ?, ?)
                ''', tuple(row))
        elif (csv_file == 'sectors.csv'):
            for _, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {table_name} 
                VALUES (?, ?, ?, ?)
                ''', tuple(row))
        else:
            print("file was not read")
        conn.commit()

    #connect to the s3 bucket
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str_blob)
    container_client = blob_service_client.get_container_client(container_name)
    
    # List of files to upload
    files_to_upload_to_blob = [ "industries.csv", "sectors.csv", "stocks.csv"]
    files_blob_upload(files_to_upload_to_blob,container_client)


    # to create the sql tables
    #create the tables the   
    #cursor.execute('''
    #CREATE TABLE Stocks (
    #    stock_id INT PRIMARY KEY,
    #    industry_id INT,
    #    sector_id INT,
    #    symbol NVARCHAR(50),
    #    name NVARCHAR(255),
    #    price FLOAT,
    #    market_cap BIGINT,
    #    last_update DATETIME
    #)
    #''')
#
    #cursor.execute('''
    #CREATE TABLE Industries (
    #    industry_id INT PRIMARY KEY,
    #    sector_id INT,
    #    name NVARCHAR(255),
    #    total_market_cap BIGINT,
    #    last_update DATETIME
    #)
    #''')
#
    #cursor.execute('''
    #CREATE TABLE Sectors (
    #    sector_id INT PRIMARY KEY,
    #    name NVARCHAR(255),
    #    total_market_cap BIGINT,
    #    last_update DATETIME
    #)
    #''')
#
    #conn.commit()

    load_csv_to_sql("stocks.csv", "Stocks", conn)
    load_csv_to_sql("industries.csv", "Industries", conn)
    load_csv_to_sql("sectors.csv", "Sectors", conn)

    cursor.close()
    conn.close()
    print("Data loaded successfully!")

    #remove the files 
    removed_files = ["stocks.csv","industries.csv","sectors.csv","industries.json","sectors.json"]
    
    for file in removed_files:
        os.remove(file)
if __name__ == "__main__":
    main()