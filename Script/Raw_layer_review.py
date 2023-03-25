from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

df = pd.read_json("E:\SUSI\PELATIHAN\DATA ENGINEER\DIGITALSKOLA\DATA\Yelp\yelp_academic_dataset_review.json", lines=True)


database = os.getenv('PG_DATABASE')
user = os.getenv('PG_USER')
passwd = os.getenv('PG_PASSWORD')
hostname = os.getenv('PG_HOSTNAME')
port = os.getenv('PG_PORT')

conn_string = f'postgresql://{user}:{passwd}@{hostname}:{port}/{database}'
db = create_engine(conn_string)


try:
    conn = psycopg2.connect(conn_string)
    
    print("Connection success")

except Exception as e:
    print(e)

cur = conn.cursor()

try:
    cur.execute("DROP TABLE IF EXISTS raw_layer.yelp_checkin")

    sql_create = """
        CREATE TABLE raw_layer.yelp_checkin(
            Date PRIMARY KEY,
            Review_id TEXT,
            Business_id TEXT,
            User_id TEXT,
            Start TEXT
        );
        """

    cur.execute(sql_create)

    conn.commit()
    # conn.close()

    print("Create table success")

except Exception as e:
    print(e)


    sql_insert = f"""
    INSERT INTO raw_layer.yelp_checkin(
        business_id,
        date_checkin
    ) VALUES %s 
    """

   
    psycopg2.extras.execute_values(cur, sql_insert, df.values)
    
    conn.commit()
    conn.close()

    cur.close()

    print("Insert to table success")
