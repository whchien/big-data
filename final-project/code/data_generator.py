import numpy as np
import pandas as pd


# FILE REQUIREMENT : "online_retail_II.csv"

def generate_tables_data(data_percentage):
    # Invoice,StockCode,Description,Quantity,InvoiceDate,Price,Customer ID,Country
    data_all_columns = pd.read_csv("online_retail_II.csv")
    data_all_columns = data_all_columns[data_all_columns['Invoice'].notna()]  # keys can't be null
    data_all_columns = data_all_columns[data_all_columns['StockCode'].notna()]  # keys can't be null
    data_all_columns = data_all_columns[data_all_columns['Customer ID'].notna()]  # keys can't be null
    data_all_columns = data_all_columns[data_all_columns['Country'].notna()]  # used in queries - field is not null
    data_all_columns = data_all_columns[data_all_columns['Price'].notna()]  # used in queries - field is not null
    data_all_columns = data_all_columns[data_all_columns['Quantity'].notna()]  # used in queries - field is not null
    data_all_columns = data_all_columns[data_all_columns['Price'] > 0]
    data_all_columns = data_all_columns[data_all_columns['Quantity'] > 0]

    # Scale the volume of data that each database contains, from 20% to 100% of the dataset with 20% intervals.
    # Cut the excess data
    number_of_rows_data_should_have = data_all_columns.shape[0] * (data_percentage / 100)
    data_all_columns = data_all_columns.iloc[:int(number_of_rows_data_should_have)]

    print("--- Initial data with all columns ---")
    print("Number of invoices: ", len(data_all_columns['Invoice'].unique()))
    print("Number of customers: ", len(data_all_columns['Customer ID'].unique()))
    print("Number of products: ", len(data_all_columns['StockCode'].unique()))
    # Invoice index column contains duplicates.
    # As the aim of the task is to replicate having data in db, content doesn't matter.
    # Therefore invoice ID is dropped and replaces with unique index (context of the rows is still duplicated).
    data_all_columns = data_all_columns.drop('Invoice', 1)
    data_all_columns = data_all_columns.reset_index().rename(columns={"index": 'Invoice'})
    print("Number of invoices now: ", len(data_all_columns['Invoice'].unique()))
    print(data_all_columns.describe())
    product_table = create_postgres_product_table(data_all_columns, data_percentage)
    customer_table = create_postgres_customer_table(data_all_columns, data_percentage)
    invoice_table = create_postgres_invoice_table(data_all_columns, data_percentage)
    create_cassandra_customer_invoice_data_csv(data_percentage, invoice_table, customer_table, product_table)
    return data_all_columns


def create_postgres_product_table(data_all_columns, data_percentage):
    data_product = data_all_columns[['StockCode', 'Description', 'Price']]
    data_product = data_product.rename(
        columns={'StockCode': 'code', 'Description': 'description', 'Price': 'unit_price'})
    data_product = data_product.sort_values(by='unit_price', ascending=False).drop_duplicates(subset='code',
                                                                                              keep='first')
    print("--- Postgres product data ---")
    print(data_product.describe())
    data_product.to_csv("postgres_product_table_%d.csv" % data_percentage, index=False)
    return data_product


# Generate random data to represent user data in database
def create_postgres_customer_table(data_all_columns, data_percentage):
    data_customer = data_all_columns[['Customer ID', 'Country']]
    data_customer = data_customer.rename(columns={'Customer ID': 'id', 'Country': 'country'})
    data_customer = data_customer.drop_duplicates(subset='id', keep="first")
    data_customer["email"] = data_customer["id"]
    data_customer['email'] = data_customer['email'].map(str)
    data_customer['email'] = data_customer['email'] + '@gmail.com'
    data_customer['phone_number'] = np.random.randint(1111111111, 9999999999, data_customer.shape[0])
    data_customer['phone_number'] = data_customer['phone_number'].map(str)
    data_customer['postcode'] = np.random.randint(11111, 99999, data_customer.shape[0])
    data_customer['postcode'] = data_customer['postcode'].map(str)
    data_customer['house_number'] = np.random.randint(1, 1000, data_customer.shape[0])
    data_customer['has_loyalty_card'] = np.where(data_customer['house_number'] % 2 == 1, True, False)
    data_customer['house_number'] = data_customer['house_number'].map(str)
    data_customer = data_customer[
        ['id', 'email', 'phone_number', 'country', 'postcode', 'house_number', 'has_loyalty_card']]
    print("--- Postgres customer data ---")
    print(data_customer.describe())
    data_customer.to_csv("postgres_customer_table_%d.csv" % data_percentage, index=False)
    return data_customer


def create_postgres_invoice_table(data_all_columns, data_percentage):
    data_invoice = data_all_columns[['Invoice', 'InvoiceDate', 'Customer ID', 'StockCode', 'Quantity']]
    data_invoice = data_invoice.rename(
        columns={'Invoice': 'id', 'InvoiceDate': 'invoice_date', 'Customer ID': 'customer_id',
                 'StockCode': 'product_code', 'Quantity': 'quantity'})
    data_invoice.to_csv("postgres_invoice_table_%d.csv" % data_percentage, index=False)
    return data_invoice


def create_cassandra_customer_invoice_data_csv(data_percentage, invoice, customer, product):
    invoice = invoice.rename(columns={'id': 'invoice_id', 'quantity': 'product_quantity'})

    customer = customer.rename(columns={'id': 'customer_id', 'email': 'customer_email',
                                        'phone_number': 'customer_phone_number', 'country': 'customer_country',
                                        'postcode': 'customer_postcode', 'house_number': 'customer_house_number',
                                        'has_loyalty_card': 'customer_has_loyalty_card'})

    product = product.rename(columns={'code': 'product_code', 'description': 'product_description',
                                      'unit_price': 'product_unit_price'})

    invoice_customer = pd.merge(invoice, customer, how="inner", on=["customer_id"])
    invoice_customer_product = pd.merge(invoice_customer, product, how="inner", on=["product_code"])

    # Pre process data for Cassandra
    # Get invoice revenue
    invoice_customer_product['invoice_total'] = invoice_customer_product['product_quantity'] * \
                                                invoice_customer_product['product_unit_price']

    print("--- Cassandra customer invoice data ---")
    print(invoice_customer_product.describe())
    invoice_customer_product.to_csv("cassandra_customer_invoice_table_%d.csv" % data_percentage, index=False)

    # Pre process data for Cassandra
    # Get daily revenue
    invoice_customer_product['invoice_date'] = pd.to_datetime(invoice_customer_product['invoice_date'])
    invoice_customer_product['invoice_date'] = invoice_customer_product['invoice_date'].dt.date
    daily_revenue = invoice_customer_product.groupby(['invoice_date'], as_index=False, sort=False).sum()
    daily_revenue = daily_revenue[['invoice_date', 'invoice_total']]
    daily_revenue = daily_revenue.rename(columns={'invoice_total': 'daily_revenue'})

    print("--- Cassandra daily revenue data ---")
    print(daily_revenue.describe())
    daily_revenue.to_csv("cassandra_daily_revenue_table_%d.csv" % data_percentage, index=False)

    # Pre process data for Cassandra
    # Get total products sold and revenue
    product_totals = invoice_customer_product.groupby(['product_code', 'product_description'], as_index=False,
                                                      sort=False).sum()
    product_totals = product_totals[['product_code', 'invoice_total', 'product_quantity', 'product_description']]
    product_totals = product_totals.rename(columns={'invoice_total': 'product_total_revenue',
                                                    'product_quantity': 'product_total_quantity_sold'})

    product_revenue = product_totals[['product_code', 'product_total_revenue', 'product_description']]
    print("--- Cassandra revenue per product data ---")
    print(product_revenue.describe())
    product_revenue.to_csv("cassandra_product_revenue_table_%d.csv" % data_percentage, index=False)

    product_sale_counts = product_totals[['product_code', 'product_total_quantity_sold', 'product_description']]
    print("--- Cassandra quantities sold per product data ---")
    print(product_sale_counts.describe())
    product_sale_counts.to_csv("cassandra_product_sale_counts_table_%d.csv" % data_percentage, index=False)


print("Generate Postgres & Cassandra table CSV files that contain 20% of the data")
generate_tables_data(20)

print("Generate Postgres & Cassandra table CSV files that contain 40% of the data")
generate_tables_data(40)

print("Generate Postgres & Cassandra table CSV files that contain 60% of the data")
generate_tables_data(60)

print("Generate Postgres & Cassandra table CSV files that contain 80% of the data")
generate_tables_data(80)

print("Generate Postgres & Cassandra table CSV files that contain 100% of the data")
generate_tables_data(100)
