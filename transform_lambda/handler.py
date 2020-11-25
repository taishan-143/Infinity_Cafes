import boto3
import csv
import json
import hashlib
import sys
from deep_getsizeof import deep_getsizeof

''' The 'raw_data' argument in the following functions is a list of lists. Each of the inner lists is a row from the .csv file:
    format = ['datetime', 'location', full name', 'drinks order', 'total amount', 'payment method', 'card number']
    e.g. ['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', ' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']
'''

# Returns a string with the format 'yyyy/mm/dd hh:mm:ss'


def get_timestamp(raw_data):
    list_of_timestamps = []
    for row in raw_data:
        date_and_time = row[0]
        date, time = date_and_time.split(' ')
        day, month, year = date.split("/")
        new_date_format = year + "/" + month + "/" + day
        timestamp = new_date_format + " " + time + ":00"
        list_of_timestamps.append(timestamp)
    return list_of_timestamps

# Creates a very unique ID


def hash_ID(input): return hashlib.md5(input.encode('utf-8')).hexdigest()

# Uses the 'hash_ID' function to create a list of very unique payment_IDs, as strings


def get_payment_IDs(raw_data):
    list_of_payment_IDs_per_order = []
    [list_of_payment_IDs_per_order.append(
        hash_ID("".join(row))) for row in raw_data]
    return list_of_payment_IDs_per_order

# Get a list of first and surnames in the same string


def get_first_name_and_surname(raw_data):
    list_of_fullnames_per_order = []
    [list_of_fullnames_per_order.append(row[2]) for row in raw_data]
    return list_of_fullnames_per_order

# returns total cost per order


def get_total_cost(raw_data):
    list_of_total_costs_per_order = []
    [list_of_total_costs_per_order.append(float(row[4])) for row in raw_data]
    return list_of_total_costs_per_order

# payment type-cash or card


def payment_type(raw_data):
    list_of_payment_types_per_order = []
    [list_of_payment_types_per_order.append(row[5]) for row in raw_data]
    return list_of_payment_types_per_order

# Gets unique location data


def get_location(raw_data):
    list_of_locations = []
    [list_of_locations.append(row[1]) for row in raw_data]
    # currently the same value in every column, per data set.
    return list_of_locations

# Returns the last 4 digits of the card numbers present in the data.
# If no such number is present, due to a cash payment being made, the value '0000' is inserted.


def get_card_number(raw_data):
    list_of_trimmed_card_numbers_per_order = []
    list_of_card_number_strings_to_integers = []
    for row in raw_data:
        if row[5] == "CARD":
            list_of_trimmed_card_numbers_per_order.append((row[6])[-4:])
        elif row[5] == "CASH":
            list_of_trimmed_card_numbers_per_order.append(f"0000")
    for number in list_of_trimmed_card_numbers_per_order:
        list_of_card_number_strings_to_integers.append(int(number))
    return list_of_card_number_strings_to_integers

# Isolates the orders and puts them into a list.
# Each order is in one long string.


def get_list_of_orders_in_a_single_string(raw_data):
    list_of_drink_orders = []
    [list_of_drink_orders.append([row[3]]) for row in raw_data]
    # format: [['name_x - flavour_x - price_x, name_y - flavour_y - price_y, ...']...]
    return list_of_drink_orders


'''The following functions are used to generate a menu of unique items,
   and inserting the value of 'no flavour' to those drinks that have no flavour.
'''
# Creates a list of unique items


# Potentially pointless? Returns a list of length of 512 which is odd. BUT maybe useful for isolating a unique menu.
def unique_orders_list(list_of_drink_orders):
    list_of_unique_orders = []
    for order in list_of_drink_orders:
        for drink in order:
            if drink not in list_of_unique_orders:
                list_of_unique_orders.append(drink)
    return list_of_unique_orders

# Separates a list of orders (single strings) into lists of strings with individual drinks, their flavour and price.


def order_string_separator(unique_items_list):
    list_of_split_unique_items = []
    [list_of_split_unique_items.append(item.split(","))
     for item in unique_items_list]
    # format: [[' name_x - flavour_x - price_x', ' name_y - flavour_y - price_y',...]]
    return list_of_split_unique_items
    # NB: extra spacing inside each string in the returned format is to be removed later

# Returns a list of unique items from the entire


def list_of_unique_products(list_of_separated_strings):
    unique_products_list = []
    for order in list_of_separated_strings:
        for item in order:
            product_stripped_of_spacing = item.lstrip(" ")
            if product_stripped_of_spacing in unique_products_list:
                continue
            else:
                unique_products_list.append(product_stripped_of_spacing)
    # format: ['name_x - flavour_x - price_x', 'name_y - flavour_y - price_y',...]
    return unique_products_list


# Returns a list of lists, where each list is a list of strings representing the drink attributes; name, flavour, and price; all titled.
def split_unique_products_by_dashes(unique_product_list):
    list_of_product_attributes = []
    for product in unique_product_list:
        name_flavour_price = product.split(" - ")
        name_flavour_price_titled = []
        for attribute in name_flavour_price:
            titled_attribute = attribute.title()
            name_flavour_price_titled.append(titled_attribute)
        list_of_product_attributes.append(name_flavour_price_titled)
    # format: [['name_x', 'flavour_x', 'price_x'],['name_y', 'flavour_y', 'price_y'],...]
    return list_of_product_attributes 

# Inserts the value 'No flavour' to any drink in the unique drinks attributes list, that only contains name and price;
# returning a list of lists of all drinks, each with a length of 3.


def append_no_flavour_to_drinks_with_no_flavour(unique_product_attribute_lists):
    for product in unique_product_attribute_lists:
        if len(product) == 2:
            product.insert(1, 'No Flavour')
    return unique_product_attribute_lists


'''isolate_unique_items_menu():- Combines the previous functions to generate a list of unique items, 
   which are lists containing: name, flavour, and price 
'''


def isolate_unique_items_menu(raw_data):
    list_of_drink_orders = get_list_of_orders_in_a_single_string(raw_data)
    unique_orders = unique_orders_list(list_of_drink_orders)
    list_of_drinks_per_order = order_string_separator(unique_orders)
    unique_products = list_of_unique_products(list_of_drinks_per_order)
    unique_product_attributes = split_unique_products_by_dashes(
        unique_products)
    final_unique_items_menu = append_no_flavour_to_drinks_with_no_flavour(
        unique_product_attributes)
    # format = [['name_x', 'flavour_x', 'price_x'], ['name_y', 'flavour_y', 'price_y'],...]
    return final_unique_items_menu


'''The following functions transform the data to be inserted into
   the orders_info table.
'''
# generate a list of strings, each string is a drink with its attributes; name, flavour, and price; joined by dashes.


def seperate_products_in_each_order_by_comma(list_of_drink_orders):
    new_orders_list = []
    for string_of_drinks in list_of_drink_orders:
        for drinks in string_of_drinks:
            list_of_drinks = drinks.split(",")
            new_orders_list.append(list_of_drinks)
    return new_orders_list

# Remove any spaces before and/or after any of the strings, within the list of orders,
# and appends the changes to a new list.


def strip_spaces_from_product_strings(list_of_drinks_lists_per_order):
    list_of_orders = []
    for drinks_list in list_of_drinks_lists_per_order:
        drinks = []
        for drink in drinks_list:
            drink_with_no_extra_spacing = drink.strip(" ")
            drinks.append(drink_with_no_extra_spacing)
        list_of_orders.append(drinks)
    # format: [['name_x - flavour_x - price_x', 'name_y - flavour_y - price_y',...]...]
    return list_of_orders


'''list_of_drink_strings_per_order():- uses the get_list_of_orders_in_a_single_string function,
   and the above 2 functions to generate a list of order lists.
'''


def list_of_drinks_per_order(raw_data):
    # generates a list of orders. Each order is a list containing one string.
    list_of_drink_orders = get_list_of_orders_in_a_single_string(raw_data)
    # splits each order into individual drinks, separated by commas.
    seperated_string = seperate_products_in_each_order_by_comma(
        list_of_drink_orders)
    # removes spaces from each string, in each order.
    all_formatted_orders = strip_spaces_from_product_strings(seperated_string)
    # format: [['name_x - flavour_x - price_x', 'name_y - flavour_y - price_y',...],...]
    return all_formatted_orders

# Processes the drink strings in the list of drink orders by stripping away spaces,
# splitting by dashes, and inserting 'No flavour' to drinks with no flavour setting.


def get_name_flavour_price_per_order(all_formatted_orders):
    list_of_drink_order_lists_split_into_attributes = []
    for order_list in all_formatted_orders:
        order = []
        for drink in order_list:
            remove_spaces = drink.strip(" ")
            separate_name_flavour_price = remove_spaces.split(" - ")
            if len(separate_name_flavour_price) == 3:
                order.append(separate_name_flavour_price)
            else:
                separate_name_flavour_price.insert(1, "No Flavour")
                order.append(separate_name_flavour_price)
        list_of_drink_order_lists_split_into_attributes.append(order)
    # format = [[['name_x', 'flavour_x', 'price_x'], ['name_y', 'flavour_y', 'price_y'],...], [[...],[...]],...]
    return list_of_drink_order_lists_split_into_attributes

# Titles all attributes of every drink in each order. 
def title_all_drink_attributes(list_of_drink_order_lists_split_into_attributes):
    list_of_titled_drink_order_lists = []
    for drink_order_list in list_of_drink_order_lists_split_into_attributes:
        list_of_titled_drink_orders = []
        for drink_attribute_lists in drink_order_list:
            list_of_titled_drinks = []
            for drink_attribute in drink_attribute_lists:
                titled_attribute = drink_attribute.title()
                list_of_titled_drinks.append(titled_attribute)
            list_of_titled_drink_orders.append(list_of_titled_drinks)
        list_of_titled_drink_order_lists.append(list_of_titled_drink_orders)
    # format = [[['Name_x', 'Flavour_x', 'Price_x'], ['Name_y', 'Flavour_y', 'Price_y'],...], [[...],[...]],...]   
    return list_of_titled_drink_order_lists

# Joins each attribute list back into a string with dashes separating the name, flavour, and price.
def join_drink_attributes_for_orders_table(list_of_drink_order_lists_split_into_attributes):
    list_of_drink_orders = []
    for drinks_order in list_of_drink_order_lists_split_into_attributes:
        drinks_in_an_order = []
        for drink_attribute_list in drinks_order:
            drink_attributes_joined = ' - '.join(drink_attribute_list)
            drinks_in_an_order.append(drink_attributes_joined)
        list_of_drink_orders.append(drinks_in_an_order)
    # format = [['name_x - flavour_x - price_x', 'name_y - flavour_y - price_y', ...], [.....],....]
    return list_of_drink_orders

# Creates a unique order ID, via the 'hash_ID' function, using the payment ID and the timestamp.


def make_orders_by_id(list_of_drink_orders, timestamps, payment_ids):
    list_of_drinks_per_order_by_order_ID = {}
    for order, timestamp, payment_id in zip(list_of_drink_orders, timestamps, payment_ids):
        ID = hash_ID("".join([payment_id, timestamp]))
        list_of_drinks_per_order_by_order_ID[ID] = order
    # format = {ID_x:['name_x', 'flavour_x', 'price_x'], ID_y:['name_y', 'flavour_y', 'price_y'],...}
    return list_of_drinks_per_order_by_order_ID

# Creates a unique product ID, via the 'hash_ID' function.


def make_products_by_id(final_unique_items_menu):
    list_of_unique_drinks_by_product_ID = {}
    for product in final_unique_items_menu:
        ID = hash_ID(" - ".join(product))
        list_of_unique_drinks_by_product_ID[ID] = product
    # format = {ID_x:['name_x', 'flavour_x', 'price_x'], ID_y:['name_y', 'flavour_y', 'price_y'],...}
    return list_of_unique_drinks_by_product_ID

# creates a new list of lists of product IDs


def isolate_product_IDs(orders_by_id, product_ids_by_product_string):
    product_ids_per_order = []  #  list of list of order product ids
    for ID, drinks in orders_by_id.items():  # orders_dict = dictionary of order_IDs against products, reversed_dict = dictionary of products against product_IDs
        product_ids = []
        for drink in drinks:
            drink_id = product_ids_by_product_string.get(drink)
            if drink_id is not None:
                product_ids.append(drink_id)
        product_ids_per_order.append(product_ids)
    # format = [[product_id_1, product_id_2, product_id_3,...],...]
    return product_ids_per_order


'''The following functions call previously defined functions
   to transform the data needed for the Orders_info table.
'''


def create_product_info_table_data(raw_data):
    unique_items_menu = isolate_unique_items_menu(raw_data)
    unique_products_by_their_IDs = make_products_by_id(unique_items_menu)
    for drink_attributes in unique_products_by_their_IDs.values():
        for attribute in drink_attributes:
            attribute.title()
    # format = {ID_x:['name_x', 'flavour_x', 'price_x'], ID_y:['name_y', 'flavour_y', 'price_y'],...}
    return unique_products_by_their_IDs


def create_dictionary_of_individual_orders(raw_data, timestamps, payment_ids):
    drinks_order_data = list_of_drinks_per_order(raw_data)
    split_drink_attributes_per_order = get_name_flavour_price_per_order(drinks_order_data)
    title_all_strings_in_all_orders = title_all_drink_attributes(split_drink_attributes_per_order)
    orders_table = join_drink_attributes_for_orders_table(title_all_strings_in_all_orders)
    products_by_order_IDs = make_orders_by_id(orders_table, timestamps, payment_ids)
    # data format returned = {order_id_x:[product_x, product_y,..], ...}  -->> product_x = 'name_x - flavour_x - price_x'
    return products_by_order_IDs


def create_reversed_dictionary_of_unique_items(raw_data):
    unique_products_list = isolate_unique_items_menu(raw_data)
    unique_product_attributes_by_their_IDs = make_products_by_id(
        unique_products_list)
    # Inverting dictionary mapping
    # TODO: Rejoining drink data is dicey as we are looking to recreate the foramt of an order item as it was originally received? <<< CHECK FOR SOLUTION.
    unique_product_IDs_by_products = {
        ' - '.join(v): k for k, v in unique_product_attributes_by_their_IDs.items()}
    # data format returned = {product_x: product_x_ID, product_y: product_y_ID, ...}  -->> product_x = 'name_x - flavour_x - price_x'
    return unique_product_IDs_by_products


def create_dictionary_of_orderIDs_and_productIDs(orders_dictionary, reversed_products_dictionary):
    list_of_product_ids_per_order = isolate_product_IDs(
        orders_dictionary, reversed_products_dictionary)
    list_of_products_ids_by_order_ids = {order_id: product_ids for order_id, product_ids in zip(
        orders_dictionary.keys(), list_of_product_ids_per_order)}
    # data format returned = {order_ID_x:[product_ID_x, product_ID_y,...], ...}
    return list_of_products_ids_by_order_ids

# Splits the data list in half, which will be sent to the load lambda in 2 messages, one for each half.


def split_list_at(data_list, size=401):
    size = size if len(data_list) > size else len(data_list)
    split = data_list[:size]
    rest = data_list[size:]
    return split, rest

# Splits the data dictionary in half, which will be sent to the load lambda in 2 messages, one for each half.


def split_dictionary_at(data_dictionary, size=401):
    size = size if len(data_dictionary) > size else len(data_dictionary)
    split = dict(list(data_dictionary.items())[:size])
    rest = dict(list(data_dictionary.items())[size:])
    return split, rest


def send_message(data, max_size=225000):
    sqs = boto3.client('sqs')

    if deep_getsizeof(data) < max_size:
        sqs.send_message(
            QueueUrl='https://sqs.eu-west-1.amazonaws.com/670948818876/TransformLoadQueueTeam2', MessageBody=json.dumps(data))
        print("Last message of record sent")
        print(json.dumps({"messageSent": "1", "recordsInMessage": len(data)}))
    else:
        # take the first X records/transactions and from the rest
        payment_ids, rest_of_payment_ids = split_list_at(data['payment_ids'])
        names, rest_of_names = split_list_at(data['full_name'])
        total_costs, rest_of_total_costs = split_list_at(data['total_cost'])
        method_of_payments, rest_of_method_of_payments = split_list_at(
            data['method_of_payment'])
        card_numbers, rest_of_card_numbers = split_list_at(data['card_number'])
        location, rest_of_location = split_list_at(data['location'])
        timestamps, rest_of_timestamps = split_list_at(data['timestamp'])
        # dictionary splits
        unique_products_by_IDs, rest_of_unique_products_by_IDs = split_dictionary_at(
            data['unique_products_by_IDs'])
        product_ids_by_order_id, rest_of_product_ids_by_order_id = split_dictionary_at(
            data['product_ids_by_order_id'])

        first_half_of_final_dictionary = {
            'payment_ids': payment_ids,
            'full_name': names,
            'total_cost': total_costs,
            'method_of_payment': method_of_payments,
            'card_number': card_numbers,
            'location': location,
            'timestamp': timestamps,
            'unique_products_by_IDs': unique_products_by_IDs,
            'product_ids_by_order_id': product_ids_by_order_id,
        }
        second_half_of_final_dictionary = {
            'payment_ids': rest_of_payment_ids,
            'full_name': rest_of_names,
            'total_cost': rest_of_total_costs,
            'method_of_payment': rest_of_method_of_payments,
            'card_number': rest_of_card_numbers,
            'location': rest_of_location,
            'timestamp': rest_of_timestamps,
            'unique_products_by_IDs': rest_of_unique_products_by_IDs,
            'product_ids_by_order_id': rest_of_product_ids_by_order_id,
        }

        sqs.send_message(
            QueueUrl='https://sqs.eu-west-1.amazonaws.com/670948818876/TransformLoadQueueTeam2', MessageBody=json.dumps(first_half_of_final_dictionary))
        print(json.dumps({"messageSent": "1", "recordsInMessage": len(first_half_of_final_dictionary)}))
        print("Message has been split and partially sent.")
        send_message(second_half_of_final_dictionary)


def start(event, context):
    for record in event['Records']:
        events = record['body']
        loading_json = json.loads(events)
        data = loading_json['responsePayload']
        print(json.dumps({"dataType": f"{type(data)}", "contents": f"{data}"}))      
        print("Data received")
        # Lists
        names = get_first_name_and_surname(data)
        print(names[0:5])
        total_costs = get_total_cost(data)
        print(total_costs[0:5])
        method_of_payments = payment_type(data)
        print(method_of_payments[0:5])
        card_numbers = get_card_number(data)
        print(card_numbers[0:5])
        timestamps = get_timestamp(data)
        print(timestamps[0:5])
        # TODO: This is a list with length of order count containing the cafe location/name for the corresponding order
        # However, the location should always be the same, can we handle this differently?
        location = get_location(data)
        print(location[0:5])
        # Dictionaries
        unique_products_by_IDs = create_product_info_table_data(data)
        print(list(unique_products_by_IDs.items())[0:5])
        payment_ids = get_payment_IDs(data)
        raw_products_per_order_by_order_id = create_dictionary_of_individual_orders(
            data, timestamps, payment_ids)
        product_id_by_unique_raw_product = create_reversed_dictionary_of_unique_items(
            data)
        product_ids_by_order_id = create_dictionary_of_orderIDs_and_productIDs(
            raw_products_per_order_by_order_id, product_id_by_unique_raw_product)
        print(list(product_ids_by_order_id.items())[0:5])

        final_dictionary = {
            'payment_ids': payment_ids,
            'full_name': names,
            'total_cost': total_costs,
            'method_of_payment': method_of_payments,
            'card_number': card_numbers,
            'location': location,
            'unique_products_by_IDs': unique_products_by_IDs,
            'product_ids_by_order_id': product_ids_by_order_id,
            'timestamp': timestamps
        }
        print(json.dumps({"dataType": f"{type(final_dictionary)}", "contents": f"{final_dictionary}"}))  
        print("Data transformed")

        send_message(final_dictionary)

        print("SUCCESS")
