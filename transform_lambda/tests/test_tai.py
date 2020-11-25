import hashlib
import unittest
from unittest.mock import Mock, patch
from transform_lambda.handler import * 
from transform_lambda.deep_getsizeof import deep_getsizeof


class Test_All_Transform_Functions(unittest.TestCase):

    def test_hash_ID_function(self):
        # Arrange
        value_to_be_hashed = 'hash me'
        expected_value = '17b31dce96b9d6c6d0a6ba95f47796fb'
        # Act
        actual_value = hash_ID(value_to_be_hashed)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_hash_ID_function:  --> SUCCESS")
        except Exception:
            print("test_hash_ID_function:  --> FAILURE")

    def test_get_payment_IDs(self):
        # Arrange
        raw_data = [['hello', 'how', 'are', 'you?'],['very', 'good', 'thank you']]
        expected_values = ['6c9eb66244c512278b2096f42a9ca1f4', 'a4d4b124db29823f29be71264f931af4']
        # Act
        actual_values = get_payment_IDs(raw_data)
        # Assert
        try:
            self.assertEqual(expected_values, actual_values)
            print("test_get_payment_IDs:  --> SUCCESS")
        except Exception:
            print("test_get_payment_IDs:  --> FAILURE")

    def test_get_total_cost(self):
        # Arrange
        raw_data_snippet = [['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', ' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']]
        expected_value = [11.00]
        # Act 
        actual_value = get_total_cost(raw_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_get_total_cost:  --> SUCCESS")
        except Exception:
            print("test_get_total_cost:  --> FAILURE")

    def test_get_location(self):
        # Arrange
        raw_data_snippet = [['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', ' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']]
        expected_value = ['Aberdeen']
        # Act
        actual_value = get_location(raw_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_get_location:  --> SUCCESS")
        except Exception:
            print("test_get_location:  --> FAILURE")

    def test_get_list_of_orders_in_a_string(self):
        # Arrange
        raw_data_snippet = [['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', ' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']]
        expected_value = [[' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85']]
        # Act
        actual_value = get_list_of_orders_in_a_single_string(raw_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_get_list_of_orders_in_a_string:  --> SUCCESS")
        except Exception:
            print("test_get_list_of_orders_in_a_string:  --> FAILURE")

    def test_order_string_separator(self):
        # Arrange 
        order_data_snippet = [' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85']
        expected_value = [[' Large Flavoured latte - Gingerbread - 2.85', ' Large Flavoured latte - Vanilla - 2.85', ' Large Latte - 2.45', ' Large Flavoured latte - Gingerbread - 2.85']]
        # Act
        actual_value = order_string_separator(order_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_order_string_separator:  --> SUCCESS")
        except Exception:
            print("test_order_string_separator:  --> FAILURE")

    def test_split_unique_products_by_dashes(self):
        # Arrange
        order_data_snippet = ['Large Flavoured latte - Gingerbread - 2.85', 'Large Flavoured latte - Vanilla - 2.85']
        expected_value = [['Large Flavoured latte', 'Gingerbread', '2.85'],['Large Flavoured latte', 'Vanilla', '2.85']]
        # Act
        actual_value = split_unique_products_by_dashes(order_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_split_unique_products_by_dashes:  --> SUCCESS")
        except Exception:
            print("test_split_unique_products_by_dashes:  --> FAILURE")

    def test_seperate_products_in_each_order_by_comma(self):
        # Arrange
        oerder_data_snippet = [[' Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85']]
        expected_value = [[' Large Flavoured latte - Gingerbread - 2.85', ' Large Flavoured latte - Vanilla - 2.85', ' Large Latte - 2.45', ' Large Flavoured latte - Gingerbread - 2.85']]
        # Act
        actual_value = seperate_products_in_each_order_by_comma(oerder_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_seperate_products_in_each_order_by_comma:  --> SUCCESS")
        except Exception:
            print("test_seperate_products_in_each_order_by_comma:  --> FAILURE")

    def test_get_name_flavour_price_per_order(self):
        # Arrange
        order_data_snippet = [['Large Flavoured latte - Gingerbread - 2.85', 'Large Flavoured latte - Vanilla - 2.85', 'Large Latte - 2.45', 'Large Flavoured latte - Gingerbread - 2.85']]
        expected_value = [[['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', 'Vanilla', '2.85'], ['Large Latte', 'No flavour', '2.45'], ['Large Flavoured latte', 'Gingerbread', '2.85']]]
        # Act
        actual_value = get_name_flavour_price_per_order(order_data_snippet)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_get_name_flavour_price_per_order:  --> SUCCESS")
        except Exception:
            print("test_get_name_flavour_price_per_order:  --> FAILURE")

    def test_split_data_list_in_half(self):
        # Arrange
        data = [1,2,3,4,5,6,7,8,9]
        expected_value = ([1,2,3,4],[5,6,7,8,9])
        # Act
        actual_value = split_data_list_in_half(data)
        # Assert
        try:
            self.assertEqual(expected_value, actual_value)
            print("test_split_data_list_in_half:  --> SUCCESS")
        except Exception:
            print("test_split_data_list_in_half:  --> FAILURE")

    @unittest.mock.patch("transform_lambda.handler.hash_ID")
    def test_make_orders_by_id(self, mock_hash_ID):
        # Arrange
        test_timestamp = '2020/11/11 16:30:00'
        test_payment_ID = hash_ID('payment_ID')
        pass
        # Act
        # Assert


if __name__=="__main__":
    unittest.main()