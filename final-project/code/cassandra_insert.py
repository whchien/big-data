from datetime import datetime

import pandas as pd
from cassandra.cqlengine.query import BatchQuery
from tqdm import tqdm

from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.auth import PlainTextAuthProvider


# Cassandra table models
class CustomerInvoice(Model):
    __key_space__ = 'bigdata'
    __table_name__ = 'customer_invoice'
    customer_id = columns.Integer(primary_key=True)
    invoice_date = columns.DateTime(primary_key=True)
    product_code = columns.Text(primary_key=True)
    invoice_id = columns.Integer(primary_key=True)
    customer_email = columns.Text()
    customer_phone_number = columns.Text()
    customer_country = columns.Text()
    customer_postcode = columns.Text()
    customer_house_number = columns.Text()
    customer_has_loyalty_card = columns.Boolean()
    product_description = columns.Text()
    product_unit_price = columns.Float()
    product_quantity = columns.Integer()
    invoice_total = columns.Float()


class DailyRevenue(Model):
    __key_space__ = 'bigdata'
    __table_name__ = 'daily_revenue'
    invoice_date = columns.Date(primary_key=True)
    daily_revenue = columns.Float(primary_key=True)


class ProductRevenue(Model):
    __key_space__ = 'bigdata'
    __table_name__ = 'product_revenue'
    product_code = columns.Text(primary_key=True)
    product_total_revenue = columns.Float(primary_key=True)
    product_description = columns.Text()


class ProductSaleCounts(Model):
    __key_space__ = 'bigdata'
    __table_name__ = 'product_sale_counts'
    product_code = columns.Text(primary_key=True)
    product_total_quantity_sold = columns.Integer(primary_key=True)
    product_description = columns.Text()


CASSANDRA_1_IP_ADDRESS = '35.209.210.66'
CASSANDRA_2_IP_ADDRESS = '35.208.184.187'
CUSTOMER_INVOICE_TABLE_FILE_NAME = 'cassandra_customer_invoice_table_20.csv'
DAILY_REVENUE_TABLE_FILE_NAME = 'cassandra_daily_revenue_table_20.csv'
PRODUCT_REVENUE_TABLE_FILE_NAME = 'cassandra_product_revenue_table_20.csv'
PRODUCT_SALE_COUNTS_FILE_NAME = 'cassandra_product_sale_counts_table_20.csv'

CASSANDRA_SETUP_KWARGS = {'protocol_version': 3,
                          "auth_provider": PlainTextAuthProvider(username='cassandra', password='cassandra')}
connection.setup(hosts=[CASSANDRA_2_IP_ADDRESS], default_keyspace="bigdata")


def populate_customer_invoice_table():
    sync_table(CustomerInvoice)
    customer_invoice = pd.read_csv(CUSTOMER_INVOICE_TABLE_FILE_NAME)
    # Insert data into Cassandra table in 100 row batches
    batch_size = 100
    batch_current_file_count = 0
    batch_manager = BatchQuery()
    for index, row in tqdm(customer_invoice.iterrows(), total=customer_invoice.shape[0]):
        CustomerInvoice.batch(batch_manager) \
            .create(customer_id=int(row['customer_id']),
                    invoice_date=datetime.strptime(row['invoice_date'], "%Y-%m-%d %H:%M:%S"),
                    product_code=str(row['product_code']),
                    invoice_id=row['invoice_id'],
                    customer_email=row['customer_email'],
                    customer_phone_number=str(row['customer_phone_number']),
                    customer_country=row['customer_country'],
                    customer_postcode=str(row['customer_postcode']),
                    customer_house_number=str(row['customer_house_number']),
                    customer_has_loyalty_card=row['customer_has_loyalty_card'],
                    product_description=row['product_description'],
                    product_unit_price=row['product_unit_price'],
                    product_quantity=row['product_quantity'],
                    invoice_total=row['invoice_total'])
        batch_current_file_count += 1
        if batch_current_file_count == batch_size:
            batch_manager.execute()
            batch_current_file_count = 0
    batch_manager.execute()


def populate_daily_revenue_table():
    sync_table(DailyRevenue)
    daily_revenue = pd.read_csv(DAILY_REVENUE_TABLE_FILE_NAME)

    batch_size = 200
    batch_current_file_count = 0
    batch_manager = BatchQuery()
    for index, row in tqdm(daily_revenue.iterrows(), total=daily_revenue.shape[0]):
        DailyRevenue.batch(batch_manager) \
            .create(invoice_date=datetime.strptime(row['invoice_date'], "%Y-%m-%d"),
                    daily_revenue=row['daily_revenue'])
        batch_current_file_count += 1
        if batch_current_file_count == batch_size:
            batch_manager.execute()
            batch_current_file_count = 0
    batch_manager.execute()


def populate_product_revenue_table():
    sync_table(ProductRevenue)
    product_revenue = pd.read_csv(PRODUCT_REVENUE_TABLE_FILE_NAME)

    batch_size = 200
    batch_current_file_count = 0
    batch_manager = BatchQuery()
    for index, row in tqdm(product_revenue.iterrows(), total=product_revenue.shape[0]):
        ProductRevenue.batch(batch_manager) \
            .create(product_code=str(row['product_code']),
                    product_total_revenue=row['product_total_revenue'],
                    product_description=row['product_description'])
        batch_current_file_count += 1
        if batch_current_file_count == batch_size:
            batch_manager.execute()
            batch_current_file_count = 0
    batch_manager.execute()


def populate_product_sale_counts_table():
    sync_table(ProductSaleCounts)
    product_sale_counts = pd.read_csv(PRODUCT_SALE_COUNTS_FILE_NAME)

    batch_size = 200
    batch_current_file_count = 0
    batch_manager = BatchQuery()
    for index, row in tqdm(product_sale_counts.iterrows(), total=product_sale_counts.shape[0]):
        ProductSaleCounts.batch(batch_manager) \
            .create(product_code=str(row['product_code']),
                    product_total_quantity_sold=row['product_total_quantity_sold'],
                    product_description=row['product_description'])
        batch_current_file_count += 1
        if batch_current_file_count == batch_size:
            batch_manager.execute()
            batch_current_file_count = 0
    batch_manager.execute()


populate_customer_invoice_table()
populate_daily_revenue_table()
populate_product_revenue_table()
populate_product_sale_counts_table()
