import psycopg2
import sys
import os
import json
import csv

import boto3
from dotenv import load_dotenv
load_dotenv()

# Gets data from s3
def read_s3_bucket():
    data_list=[]
    s3 = boto3.resource('s3')
    obj = s3.Object('cafe-data-data-pump-dev-team-2','aberdeen_11-10-2020_19-49-26.csv')
    data = obj.get()['Body'].read().decode('utf-8').splitlines()
    lines = csv.reader(data)
    for line in lines:
        data_list.append(line)
    return data_list

# Removed get_date_time and replaced with new function that 
# returns a string with yyyy/mm/dd hh:mm:ss
def get_timestamp(list1):
    date_list = []      
    for row in list1:        
        date_time = row[0]
        date, time = date_time.split(' ')
        day, month, year = date.split("/")
        new_date = year + "/" + month + "/" + day 
        timestamp = new_date + " "  + time + ":00"
        date_list.append(timestamp)    
    return date_list

#Get first and lastname
def get_first_name_and_surname(list1):
    name_list =[]
    for row in list1:
        first_and_surname = row[2]        
        name_list.append(first_and_surname)
    return name_list

#returns total cost per order
def get_total_cost(list1):
    total_cost =[]
    for row in list1:
        total_cost.append(float(row[4]))
    return total_cost

#payment type-cash or card
def payment_type(list1):
    payment_type_list=[]
    for row in list1:
        payment_type_list.append(row[5])
    return payment_type_list

# Gets unique location data
def get_location(list1):
    location_list = []
    for row in list1:
        location_list.append(row[1])            
    return location_list

def get_card_number(list1):
    trim = []
    card_numbers = []
    for line in list1:
        if line[5] == "CARD":
            trim.append(str(line[6])[-4:])
        elif line[5] == "CASH":
            trim.append(f"0000")        
    for number in trim:
        card_numbers.append(int(number))
    return card_numbers
        
    
# Isolates the orders and puts them into a list
def get_list_of_orders_in_a_string(list1):
    new_list = []
    for item in list1:
        new_list.append([item[3]])
    return new_list

# Creates a list of unique items
def unique_items(list1):
    item_list = []
    for item in list1:
        for x in item:
            if x not in item_list:
                item_list.append(x)
    return item_list

# Separates strings into lists with drink, flavour and price
def string_separator(list1):
    item_list = []
    for item in list1:
        x = item.split(",")
        item_list.append(x)        
    return item_list

def menu1(list1):
    menu = []
    for order in list1:
        for item in order:
            x = item.lstrip(" ")
            if x in menu:
                continue
            else:
                menu.append(x)
    return menu

def menu2(list1):
    new_list = []    
    for item1 in list1:
        x = item1.split(" - ")
        new_list.append(x)    
    return new_list


def flav_no_flav(list1):
    for item in list1:
        if len(item) == 2:
            item.insert(1, 'No flavour')
    return list1

'''isolate_menu():- Combines the previous functions to generate a list of unique items, 
   which are lists containing: name, flavour, and price 
'''
def isolate_unique_items_menu(s3_data):
    trimmed_data = get_list_of_orders_in_a_string(s3_data)
    item_unique = unique_items(trimmed_data)
    seperated_string = string_separator(item_unique)
    menu_1 = menu1(seperated_string)
    final_menu = menu2(menu_1)
    actual_final = flav_no_flav(final_menu)
    return actual_final # format = [['name_x', 'flavour_x', 'price_x'], ['name_y', 'flavour_y', 'price_y'],...]


'''The following functions transform the data to be inserted into
   the orders_info table.
'''

# generate list of strings
def seperate_products_in_each_order(list1):  
    new_item_list = []
    for item_list in list1:
        for item in item_list:
            order_item = item.split(",")
            new_item_list.append(order_item)        
    return new_item_list

# remove any spaces before and after the strings
def strip_spaces_from_product_string(list1):  
    menu = []
    for order in list1:
        drinks = []
        for item in order:
            drink = item.strip(" ")
            drinks.append(drink)
        menu.append(drinks)
    return menu 

'''list_of_lists_of_drink_orders():- uses the get_list_of_orders_in_a_string function and the above 2 functions to generate a list of order lists
   in the format: [['name_x - flavour_x - price_x'], ['name_y - flavour_y - price_y'],...]
'''
def list_of_lists_of_drink_orders(data):
    trimmed_data = get_list_of_orders_in_a_string(data)  # generates a list of list of orders 
    seperated_string = seperate_products_in_each_order(trimmed_data) # splits individual drinks up by comma
    menu = strip_spaces_from_product_string(seperated_string) # removes spaces from strings 
    return menu   

# cleans data by stripping away spaces, splitting by dashes, and inserting 'No flavour' to items with no flavour setting.
def get_name_flavour_price_per_order(list1):   
    menu = []                                  
    for item in list1:   
        order = []    
        for product in item:          
            remove_spaces = product.strip(" ")
            separate_name_flavour_price = remove_spaces.split(" - ")
            if len(separate_name_flavour_price) == 3:
                order.append(separate_name_flavour_price)
            else:
                separate_name_flavour_price.insert(1, "No flavour")
                order.append(separate_name_flavour_price) 
        menu.append(order)         
    return menu   # format = [[['name', 'flavour', 'price'], ['name', 'flavour', 'price']], [[...],[...]],..]

# joins each item list back with dashes to generate a string,
# with the previously appended 'no flavour' filling the gaps for those that need it.
def join_data_for_orders_table(list_of_lists):   
    menu_list = []                                      
    for list1 in list_of_lists: 
        item_list = []
        for order in list1:
            new_value = ' - '.join(order)  
            item_list.append(new_value)
        menu_list.append(item_list)
    return menu_list        # format = [['name_x - flavour_x - price_x', 'name_y - flavour_y - price_y', ...], [.....],....]

# generates dictionary of ID's against products
def create_ID_and_items_dictionary(menu_list):  
    final_data = {}
    for ID, items in enumerate(menu_list, 1):
        final_data[ID] = items
    return final_data # format = {ID_x:['name_x', 'flavour_x', 'price_x'], ID_y:['name_y', 'flavour_y', 'price_y'],...}

# joins the name, flavour, and price together by dashes
def join_value_items_in_dictionary(dictionary_data):   
    new_orders_check = {}
    for key, value in dictionary_data.items():
        new_value = ' - '.join(value)
        new_orders_check[key] = new_value
    return new_orders_check # format = {ID_x:['name_x - flavour_x - price_x'], ID_y:['name_y - flavour_y - price_y'], ...}

# swaps the key and value around to make the ID accessible
def reverse_key_and_value(dictionary):   
    reversed_dict = {}
    for ID, Product in dictionary.items():
        reversed_dict[Product] = ID
    return reversed_dict  # format = {'name_x - flavour_x - price_x':ID_x, 'name_y - flavour_y - price_y':ID_y,...}

# creates a new list of lists of product IDs
def isolate_product_IDs(orders_dict, reversed_dict):  
    list_of_lists_of_IDs = []
    for ID, drinks in orders_dict.items():  # orders_dict = dictionary of order_IDs against products, reversed_dict = dictionary of products against product_IDs
        ID_list = []
        for drink in drinks:
            if drink in reversed_dict.keys():  
                ID_list.append(reversed_dict[drink])
        list_of_lists_of_IDs.append(ID_list)
    return list_of_lists_of_IDs  # format = [[product_id_1, product_id_2, product_id_3,...],...]


'''The following functions call previously defined functions
   to transform the data needed for the Orders_info table.
'''

def create_dictionary_of_individual_orders(s3_data):  
    order_data = list_of_lists_of_drink_orders(s3_data) 
    clean_data = get_name_flavour_price_per_order(order_data)
    orders_table = join_data_for_orders_table(clean_data)   
    orders_dictionary = create_ID_and_items_dictionary(orders_table)
    return orders_dictionary  # data format returned = {order_id_x:[product_x, product_y,..], ...}

def create_reversed_dictionary_of_unique_items(s3_data):  
    list_unique_items = isolate_unique_items_menu(s3_data)
    unique_drinks_dictionary = create_ID_and_items_dictionary(list_unique_items)
    joined_dictionary = join_value_items_in_dictionary(unique_drinks_dictionary)
    reversed_dictionary = reverse_key_and_value(joined_dictionary)  
    return reversed_dictionary  # data format returned = {product_x: product_x_ID, product_y: product_y_ID, ...}

def create_dictionary_of_orderIDs_and_productIDs(orders_dictionary, reversed_dictionary): 
    list_of_lists_of_IDs_per_order = isolate_product_IDs(orders_dictionary, reversed_dictionary)
    ID_dict = create_ID_and_items_dictionary(list_of_lists_of_IDs_per_order)
    return ID_dict  # data format returned = {order_ID_x:[product_ID_x, product_ID_y,...], ...}



def start(event, context):
    print("Running...")

    print(os.getenv("DB_HOST"))
    print(int(os.getenv("DB_PORT")))
    print(os.getenv("DB_USER"))
    print(os.getenv("DB_PASS"))
    print(os.getenv("DB_NAME"))
    print(os.getenv("DB_CLUSTER"))

    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    user = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    cluster = os.getenv("DB_CLUSTER")

    try:
        client = boto3.client('redshift')
        creds = client.get_cluster_credentials(# Lambda needs these permissions as well DataAPI permissions
            DbUser=user,
            DbName=db,
            ClusterIdentifier=cluster,
            DurationSeconds=3600) # Length of time access is granted
    except Exception as ERROR:
        print("Credentials Issue: " + str(ERROR))
        sys.exit(1)

    print('got credentials')

    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
    except Exception as ERROR:
        print("Connection Issue: " + str(ERROR))
        sys.exit(1)

    print('connected')
    
## data to be used by all insert functions ###

    # data = read_s3_bucket()

    # #'''PAYMENT INFO TABLE'''
    # full_name = get_first_name_and_surname(data)
    # total_cost = get_total_cost(data) 
    # method_of_payment = payment_type(data)
    # card_number = get_card_number(data)
    # location = get_location(data)
    # combined = list(zip(full_name, total_cost, method_of_payment, card_number, location))
    # try:
    #     cursor = conn.cursor()
    #     for payment_info in combined:            
    #         cursor.execute("INSERT INTO payment_info (full_name, total_amount, payment_method, card_number, cafe_location) VALUES (%s, %s, %s, %s, %s)", payment_info)          
    #     cursor.close()
    #     conn.commit()
    #     print("PAYMENT INFORMATION ENTERED SUCCESSFULLY")
    # except Exception as ERROR:
    #     print("Execution Issue: " + str(ERROR))
    #     conn.close()
    #     sys.exit(1)

    # ### check for repeat drinks in product info table!!! ###
    
    # #''' PRODUCT INFO TABLE '''
    # try:
    #     transformed_menu = isolate_unique_items_menu(data)
    #     cursor = conn.cursor()
    #     for item in transformed_menu:
    #         drinks = (item[0], item[1], float(item[2]))
    #         cursor.execute("INSERT INTO product_info (name, flavour, price) VALUES (%s, %s, %s)", drinks)          
    #     cursor.close()
    #     conn.commit()
    #     print("PRODUCT INFORMATION ENTERED SUCCESSFULLY")
    # except Exception as ERROR:
    #     print("Execution Issue: " + str(ERROR))
    #     sys.exit(1)
    #     conn.close()
    

    # #''' ORDERS INFO TABLE'''
    # order_dict = create_dictionary_of_individual_orders(data)
    # reverse_dict = create_reversed_dictionary_of_unique_items(data)
    # order_and_product_IDs = create_dictionary_of_orderIDs_and_productIDs(order_dict, reverse_dict)
    # try:
    #     cursor = conn.cursor()
    #     for order_ID, product_IDs in order_and_product_IDs.items():
    #         for product in product_IDs:
    #             order_info = (order_ID, product)
    #             cursor.execute("INSERT INTO orders_info (order_id, product_id) VALUES (%s, %s)", order_info)          
    #     cursor.close()
    #     conn.commit()
    #     print("ORDER INFORMATION ENTERED SUCCESSFULLY")
    # except Exception as ERROR:
    #     print("Execution Issue: " + str(ERROR))
    #     sys.exit(1)
    #     conn.close()    

    # ''' RECEIPT TABLE'''
    # timestamp = get_timestamp(data)
    # try:
    #     cursor = conn.cursor()
    #     cursor.execute("SELECT payment_id FROM payment_info")
    #     payment_id_list = cursor.fetchall()
    #     receipt_info = zip(payment_id_list, timestamp)
    #     for pay_id, date_time in receipt_info:
    #         info_to_insert = (pay_id, date_time)            
    #         cursor.execute("INSERT INTO receipt (payment_id, date_time) VALUES (%s, %s);", info_to_insert)
    #     cursor.close()
    #     conn.commit()
    #     print("RECEIPTS ENTERED SUCCESSFULLY")
    # except Exception as ERROR:
    #     print("Execution Issue: " + str(ERROR))
    #     conn.close()
    #     sys.exit(1)
    

    # print('executed system')
    # conn.close()
