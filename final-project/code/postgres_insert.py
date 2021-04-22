import psycopg2
import psycopg2.extras as extras
import pandas as pd


def execute_batch(conn, df, table, insert_query, page_size=1000):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = insert_query % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


POSTGRES_1_IP_ADDRESS = '35.208.251.185'
POSTGRES_2_IP_ADDRESS = '35.209.208.246'
PRODUCT_TABLE_FILE_NAME = 'postgres_product_table_20.csv'
CUSTOMER_TABLE_FILE_NAME = 'postgres_customer_table_20.csv'
INVOICE_TABLE_FILE_NAME = 'postgres_invoice_table_20.csv'

# Connect to the database
connection = psycopg2.connect(host=POSTGRES_2_IP_ADDRESS,
                              user='postgres',
                              password='moomins',
                              database='postgres')
print("Connected")

print("START populating product table")
table = pd.read_csv(PRODUCT_TABLE_FILE_NAME)
product_insert_query = "INSERT INTO %s(%s) VALUES(%%s, %%s, %%s)"
execute_batch(connection, table, "product", product_insert_query)
print("FINISHED populating product table")

print("START populating customer table")
table = pd.read_csv(CUSTOMER_TABLE_FILE_NAME)
customer_insert_query = "INSERT INTO %s(%s) VALUES(%%s, %%s, %%s, %%s, %%s, %%s, %%s)"
execute_batch(connection, table, "customer", customer_insert_query)
print("FINISHED populating customer table")

print("START populating invoice table")
table = pd.read_csv(INVOICE_TABLE_FILE_NAME)
invoice_insert_query = "INSERT INTO %s(%s) VALUES(%%s, %%s, %%s, %%s, %%s)"
execute_batch(connection, table, "invoice", invoice_insert_query)
print("FINISHED populating invoice table")
