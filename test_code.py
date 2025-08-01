#import boto3
import csv
import json
import hashlib
import sys
from transform_lambda.deep_getsizeof import *


def get_data(file):
    cafe_data_list = []
    try:
        with open(f"{file}", "r") as cafe_data:
            x = csv.reader(cafe_data)
            for row in x:
                cafe_data_list.append(row)
        return cafe_data_list
    except:
        print("Error loading file.")


''' The 'raw_data' argument in the following functions is a list of lists. Each of the inner lists is a row from the .csv file'''

# Removed get_date_time and replaced with new function that
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
    [list_of_payment_IDs_per_order.append(hash_ID("".join(row))) for row in raw_data]
    return list_of_payment_IDs_per_order


data = get_data("Data/aberdeen_11-10-2020_19-49-26.csv")
time_stamps = get_timestamp(data)
pay_IDs = get_payment_IDs(data)


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
    return list_of_locations # currently the same value in every column, per data set.

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
def unique_orders_list(list_of_drink_orders):  ## Potentially pointless? Returns a list of length of 512 which is odd. BUT maybe useful for isolating a unique menu.
    list_of_unique_orders = []
    for order in list_of_drink_orders:
        for drink in order:
            if drink not in list_of_unique_orders:
                list_of_unique_orders.append(drink)
    return list_of_unique_orders

# Separates a list of orders (single strings) into lists of strings with individual drinks, their flavour and price.
def order_string_separator(unique_items_list): 
    list_of_split_unique_items = []
    [list_of_split_unique_items.append(item.split(",")) for item in unique_items_list]
    # format: [[' name_x - flavour_x - price_x', ' name_y - flavour_y - price_y',...]] 
    return list_of_split_unique_items 
    ## NB: extra spacing inside each string in the returned format is to be removed later

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
    unique_product_attributes = split_unique_products_by_dashes(unique_products)
    final_unique_items_menu = append_no_flavour_to_drinks_with_no_flavour(unique_product_attributes)
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
    list_of_drink_orders = get_list_of_orders_in_a_single_string(raw_data)  # generates a list of orders. Each order is a list containing one string. 
    seperated_string = seperate_products_in_each_order_by_comma(list_of_drink_orders)  # splits each order into individual drinks, separated by commas.
    all_formatted_orders = strip_spaces_from_product_strings(seperated_string)  # removes spaces from each string, in each order.
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

a = create_dictionary_of_individual_orders(data, time_stamps, pay_IDs)
print(a)

def create_reversed_dictionary_of_unique_items(raw_data):
    unique_products_list = isolate_unique_items_menu(raw_data)
    unique_product_attributes_by_their_IDs = make_products_by_id(unique_products_list)
    # Inverting dictionary mapping
    # TODO: Rejoining drink data is dicey as we are looking to recreate the foramt of an order item as it was originally received? <<< CHECK FOR SOLUTION.
    unique_product_IDs_by_products = {' - '.join(v): k for k, v in unique_product_attributes_by_their_IDs.items()}
    # data format returned = {product_x: product_x_ID, product_y: product_y_ID, ...}  -->> product_x = 'name_x - flavour_x - price_x'
    return unique_product_IDs_by_products  

def create_dictionary_of_orderIDs_and_productIDs(orders_dictionary, reversed_products_dictionary):
    list_of_product_ids_per_order = isolate_product_IDs(orders_dictionary, reversed_products_dictionary)
    list_of_products_ids_by_order_ids = {order_id: product_ids for order_id, product_ids in zip(orders_dictionary.keys(), list_of_product_ids_per_order)}
    # data format returned = {order_ID_x:[product_ID_x, product_ID_y,...], ...}
    return list_of_products_ids_by_order_ids   

# Splits the data list in half, which will be sent to the load lambda in 2 messages, one for each half. 
def split_data_list_in_half(data_list):
    first_half_of_data = data_list[:len(data_list)//2]
    second_half_of_data = data_list[len(data_list)//2:]
    if len(first_half_of_data) + len(second_half_of_data) == len(data_list):
        print("Split Successful")
    else:
        print("Split Failed")
    # Generates a tuple of lists
    return (first_half_of_data, second_half_of_data)

# Splits the data dictionary in half, which will be sent to the load lambda in 2 messages, one for each half.
def split_data_dictionary_in_half(data_dictionary):
    first_half_of_data = dict(list(data_dictionary.items())[:len(data_dictionary)//2])
    second_half_of_data = dict(list(data_dictionary.items())[len(data_dictionary)//2:])
    if len(first_half_of_data) + len(second_half_of_data) == len(data_dictionary):
        print("Split Successful")
    else:
        print("Split Failed")
    # Generate a tuple of dictionaries 
    return (first_half_of_data, second_half_of_data)


names = get_first_name_and_surname(data)
total_costs = get_total_cost(data)
method_of_payments = payment_type(data)
card_numbers = get_card_number(data)
timestamps = get_timestamp(data)
# TODO: This is a list with length of order count containing the cafe location/name for the corresponding order
# However, the location should always be the same, can we handle this differently?
location = get_location(data)
unique_products_by_IDs = create_product_info_table_data(data) 
payment_ids = get_payment_IDs(data)
raw_products_per_order_by_order_id = create_dictionary_of_individual_orders(data, timestamps, payment_ids)
product_id_by_unique_raw_product = create_reversed_dictionary_of_unique_items(data)
product_ids_by_order_id = create_dictionary_of_orderIDs_and_productIDs(raw_products_per_order_by_order_id, product_id_by_unique_raw_product)

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



# list splits
payment_ids_split = split_data_list_in_half(payment_ids)
names_split = split_data_list_in_half(names)
total_costs_split = split_data_list_in_half(total_costs)
method_of_payments_split = split_data_list_in_half(method_of_payments)
card_numbers_split = split_data_list_in_half(card_numbers)
location_split = split_data_list_in_half(location)
timestamps_split = split_data_list_in_half(timestamps)
# dictionary splits
unique_products_by_IDs_split = split_data_dictionary_in_half(unique_products_by_IDs)
product_ids_by_order_id_split = split_data_dictionary_in_half(product_ids_by_order_id)

first_half_of_final_dictionary = {
    'payment_ids': payment_ids_split[0],
    'full_name': names_split[0],
    'total_cost': total_costs_split[0],
    'method_of_payment': method_of_payments_split[0],
    'card_number': card_numbers_split[0],
    'location': location_split[0],
    'unique_products_by_IDs': unique_products_by_IDs_split[0],
    'product_ids_by_order_id': product_ids_by_order_id_split[0],
    'timestamp': timestamps_split[0]
}
second_half_of_final_dictionary = {
    'payment_ids': payment_ids_split[1],
    'full_name': names_split[1],
    'total_cost': total_costs_split[1],
    'method_of_payment': method_of_payments_split[1],
    'card_number': card_numbers_split[1],
    'location': location_split[1],
    'unique_products_by_IDs': unique_products_by_IDs_split[1],
    'product_ids_by_order_id': product_ids_by_order_id_split[1],
    'timestamp': timestamps_split[1]
}



