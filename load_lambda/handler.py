import psycopg2
import sys
import os
import json
import csv
import psycopg2.extras

import boto3
from dotenv import load_dotenv
load_dotenv()

def start(event, context):
    print("Load starting...")
    print(json.dumps({"dataType": f"{type(event)}", "contents": f"{event}"})) 
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    user = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    cluster = os.getenv("DB_CLUSTER")

    try:
        client = boto3.client('redshift')
        creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions
            DbUser=user,
            DbName=db,
            ClusterIdentifier=cluster,
            DurationSeconds=3600)  # Length of time access is granted
        print(host)
        print(port)
        print(user)
        print(db)
        print(cluster)
        print('got credentials')
    except Exception as ERROR:
        print("Credentials Issue: " + str(ERROR))
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
        print('connected')
    except Exception as ERROR:
        print("Connection Issue: " + str(ERROR))
        sys.exit(1)

    for record in event['Records']:
        message_body = record['body']
        print(json.dumps({"messageReceived": "1", "recordsInMessage": len(message_body)}))
        loading_json = json.loads(message_body)
        data_dictionary = loading_json
        print(json.dumps({"dataType": f"{type(data_dictionary)}", "contents": f"{data_dictionary}"})) 

        print("Data received")

    ## data to be used by all insert functions ###

        #'''PAYMENT INFO TABLE'''
        # TODO: This should work
        payment_ids = data_dictionary['payment_ids']
        print(payment_ids[0:5])
        # END
        full_names = data_dictionary['full_name']
        print(full_names[0:5])
        total_costs = data_dictionary['total_cost']
        print(total_costs[0:5])
        method_of_payments = data_dictionary['method_of_payment']
        print(method_of_payments[0:5])
        card_numbers = data_dictionary['card_number']
        print(card_numbers[0:5])
        locations = data_dictionary['location']
        print(locations[0:5])
        # This contains list of unique products from order set
        # TODO: Do we need an id here?
        # [ {'<product_id>': '<name>', '<flavour>', '<price>'} ]
        unique_products_by_IDs = data_dictionary['unique_products_by_IDs']
        print(list(unique_products_by_IDs.items())[0:5])
        
        try:
            cursor = conn.cursor()
            all_data_in_one =[]
            for payment_info in zip(payment_ids, full_names, total_costs, method_of_payments, card_numbers, locations):
                all_data_in_one.append(payment_info)
                # TODO: If we insert payment_id we need to alter the table to not auto increment
            psycopg2.extras.execute_values(cursor,
                                           "INSERT INTO payment_info (payment_id, full_name, total_amount, payment_method, card_number, cafe_location) VALUES %s", all_data_in_one, page_size=1000)
            cursor.close()
            conn.commit()
            print("PAYMENT INFORMATION ENTERED SUCCESSFULLY")
        except Exception as ERROR:
            print("Execution Issue: " + str(ERROR))
            conn.close()
            sys.exit(1)

        # TODO: If we set our own product id we can safely overwrite products every time, I think?
        ### check for repeat drinks in product info table!!! ###

        #''' PRODUCT INFO TABLE '''
        try:
            cursor = conn.cursor()
            # TODO: Look at inserting mutiple rows in one INSERT statement
            all_data_in_one=[]
            for ID, product in unique_products_by_IDs.items():
                # product_id is a hash of product fields so we can safely overwrite the products 
                # TODO: If we insert payment_id we need to alter the table to not auto increment
                drinks = (ID, product[0], product[1], float(product[2]))
                all_data_in_one.append(drinks)
                # ACTUALLY TODO: This will throw is the product_id exists in table - we need to update on conflict in Redshift style.
                # In MySQL this is INSERT INTO ... ON DUPLICATE KEY EXISTS UPDATE ...
            cursor.execute("TRUNCATE TABLE product_info_staging")
            psycopg2.extras.execute_values(cursor,
                                           "INSERT INTO product_info_staging (product_id, name, flavour, price) VALUES %s", all_data_in_one, page_size=1000)
            cursor.execute("DELETE FROM product_info USING product_info_staging WHERE product_info.product_id = product_info_staging.product_id")
            cursor.execute("INSERT INTO product_info  SELECT * FROM product_info_staging")
            cursor.close()
            conn.commit()
            print("PRODUCT INFORMATION ENTERED SUCCESSFULLY")
        except Exception as ERROR:
            print("Execution Issue: " + str(ERROR))
            sys.exit(1)
            conn.close()

        #''' ORDERS INFO TABLE'''
        order_and_product_IDs = data_dictionary['product_ids_by_order_id']
        print(list(order_and_product_IDs.items())[0:5])
        try:
            cursor = conn.cursor()
            # TODO: Look at inserting mutiple rows in one INSERT statement
            all_data_in_one=[]
            for order_ID, product_IDs in order_and_product_IDs.items():
                for product in product_IDs:
                    order_info = (order_ID, product)
                    all_data_in_one.append(order_info)
            psycopg2.extras.execute_values(cursor,
                "INSERT INTO orders_info (order_id, product_id) VALUES %s", all_data_in_one, page_size=1000)
            cursor.close()
            conn.commit()
            print("ORDER INFORMATION ENTERED SUCCESSFULLY")
        except Exception as ERROR:
            print("Execution Issue: " + str(ERROR))
            sys.exit(1)
            conn.close()

        #''' RECEIPT TABLE'''
        timestamp = data_dictionary['timestamp']
        print(timestamp[0:5])
        try:
            cursor = conn.cursor()
            # cursor.execute("SELECT payment_id FROM payment_info")
            # payment_id_list = cursor.fetchall()
            all_data_in_one=[]
            payment_id_list = payment_ids
            receipt_info = zip(order_and_product_IDs.keys(), payment_id_list, timestamp)
            for order_id, pay_id, date_time in receipt_info:
                info_to_insert = (order_id, pay_id, date_time)
                all_data_in_one.append(info_to_insert)
                # TODO: If we insert order_id we need to alter the table to not auto increment

            psycopg2.extras.execute_values(cursor,
                                           "INSERT INTO receipt (order_id, payment_id, date_time) VALUES %s", all_data_in_one, page_size=1000)
            cursor.close()
            conn.commit()
            print("RECEIPTS ENTERED SUCCESSFULLY")
        except Exception as ERROR:
            print("Execution Issue: " + str(ERROR))
            conn.close()
            sys.exit(1)

        print('SUCCESS')
    conn.close()
