import pandas as pd
import mysql.connector
from datetime import datetime

try:
    # Step 1: Load CSV (use your file path)
    df = pd.read_csv("olist_customers_dataset.csv")

    # Step 2: Clean Data
    df.columns = df.columns.str.lower()

    df = df.dropna()
    df = df.drop_duplicates()

    df.rename(columns={
        "customer_zip_code_prefix": "zip_code",
        "customer_city": "city",
        "customer_state": "state"
    }, inplace=True)

    # Step 3: Connect MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="ecommerce_db"
    )
    cursor = conn.cursor()

    # Step 4: Insert Data
    for _, row in df.iterrows():
        cursor.execute(
            """INSERT INTO customers 
            (customer_id, customer_unique_id, zip_code, city, state) 
            VALUES (%s, %s, %s, %s, %s)""",
            (
                row['customer_id'],
                row['customer_unique_id'],
                int(row['zip_code']),
                row['city'],
                row['state']
            )
        )

    conn.commit()

    # Step 5: Logging
    with open("log.txt", "a") as f:
        f.write(f"{datetime.now()} - Success\n")

except Exception as e:
    with open("log.txt", "a") as f:
        f.write(f"{datetime.now()} - Error: {e}\n")

finally:
    print('Process completed')