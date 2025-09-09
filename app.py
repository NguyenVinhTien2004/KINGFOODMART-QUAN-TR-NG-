import rpa as r
import tagui as t
import random
import time
from urllib.parse import urlparse
from product_fetcher import fetch_and_save_products
from pymongo import MongoClient
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.auth import default
import json
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
BIGQUERY_TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")

client = MongoClient(MONGO_URI)
db = client["db_kf"]
collection = db["kf_new"]

credentials, project = default()

bigquery_client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

def random_sleep(lower_limit, upper_limit):
    if lower_limit > upper_limit:
        raise ValueError("Lower limit cannot be greater than upper limit.")
    sleep_seconds = random.uniform(lower_limit, upper_limit)
    time.sleep(sleep_seconds)
    return sleep_seconds

def escape_string(value):
    if value:
        return value.replace("'", " ")
    return value

def upload_to_bigquery(data):
    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("stock_quantity", "FLOAT"),
        bigquery.SchemaField("total_sold", "FLOAT"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("original_price", "FLOAT"),
        bigquery.SchemaField("promotion", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField(
            "sales_history",
            "RECORD",
            mode="REPEATED", 
            fields=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("sold_in_date", "FLOAT"),
            ],
        ),
        bigquery.SchemaField(
            "stock_history",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("stock_increased", "FLOAT"),
                bigquery.SchemaField("stock_decreased", "FLOAT"),
            ],
        ),
         bigquery.SchemaField(
            "price_history",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("original_price", "FLOAT"),
            ],
        ),
    ]

    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"
    
    try:
        table = bigquery_client.get_table(table_id)
        print(f"Table {table_id} already exists.")
        
        current_schema = table.schema
        if current_schema != schema:
            print(f"Schema mismatch. Updating schema...")
            raise Exception("Schema mismatch, manual update required.")
        
    except NotFound:
        table_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TABLE_ID)
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print(f"Table {table_id} created.")

    for item in data:
        item["promotion"] = escape_string(item["promotion"])
        item["description"] = escape_string(item["description"])
        item["name"] = escape_string(item["name"])
        sales_history = ", ".join([
            f"STRUCT(DATE('{entry['date']}'), {float(entry['sold_in_date'])})" 
            for entry in item.get("sales_history", [])
        ])
        
        stock_history = ", ".join([
            f"STRUCT(DATE('{entry['date']}'), {float(entry['stock_increased'])}, {float(entry['stock_decreased'])})" 
            for entry in item.get("stock_history", [])
        ])
        price_history = ", ".join([
            f"STRUCT(DATE('{entry['date']}'), {float(entry['price'])}, {float(entry['original_price'])})" 
            for entry in item.get("price_history", [])
        ])
        query = f"""
        MERGE INTO `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}` T
        USING (SELECT '{item["id"]}' AS id,
                      '{item["name"]}' AS name,
                      {float(item["stock_quantity"])} AS stock_quantity,
                      {float(item["total_sold"])} AS total_sold,
                      {float(item["price"])} AS price,
                      {float(item["original_price"])} AS original_price,
                      '{item["promotion"]}' AS promotion,
                      '{item["description"]}' AS description,
                      DATE('{item["date"]}') AS date,
                      ARRAY[{sales_history}] AS sales_history,
                      ARRAY[{stock_history}] AS stock_history,
                      ARRAY[{price_history}] AS price_history) S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET 
            name = S.name,
            stock_quantity = S.stock_quantity,
            total_sold = S.total_sold,
            price = S.price,
            original_price = S.original_price,
            promotion = S.promotion,
            description = s.description,
            date = S.date,
            sales_history = S.sales_history,
            stock_history = S.stock_history,
            price_history = S.price_history
        WHEN NOT MATCHED THEN
          INSERT (id, name, stock_quantity, total_sold, price, original_price, promotion, description, date, sales_history, stock_history, price_history)
          VALUES (S.id, S.name, S.stock_quantity, S.total_sold, S.price, S.original_price, S.promotion,S.description, S.date, S.sales_history, S.stock_history, S.price_history)
        """

        query_job = bigquery_client.query(query)
        query_job.result() 
        
        print(f"Upsert operation for id {item['id']} completed.")

def fetch_mongo_data():
    cursor = collection.find()
    product_data_list = []

    for product in cursor:
        product["_id"] = str(product["_id"]) 
        product_data_list.append(product)

    return product_data_list

def run_rpa_tagui_script():
    t.init(visual_automation=True)
    r.url("https://www.myip.com")
    my_ip = r.read('//*[@id="ip"]')
    my_new_ip = r.load("ip_proxy.txt")
    category_urls = r.load("category_url.txt").splitlines()
    random.shuffle(category_urls)
    try:
        for url in category_urls:
            if my_ip == my_new_ip:
                r.url(url)
                parsed_url = urlparse(url)
                slug = parsed_url.path.strip("/").split("/")[-1]
                print(f"Slug: {slug}")

                r.wait(random_sleep(1.5, 2))

                total_products_str = r.read('//*[@id="__next"]/div[1]/main/div/div[4]/div[2]/div[2]/div/div[6]/div[1]/span')
                total_products = int(total_products_str)
                r.wait(random_sleep(9, 10))
                fetch_and_save_products(start_page=1, end_page=1, limit_value=total_products, slug_value=slug)
    except Exception as e:
        print(f"Error URL {url}: {str(e)}")
    finally:
        r.close()

def run_bigquery_upload():
    product_data = fetch_mongo_data()
    upload_to_bigquery(product_data)

if __name__ == "__main__":
    try:
        run_rpa_tagui_script()  
        run_bigquery_upload()  
    except Exception as e:
        print(f"An error occurred: {str(e)}")