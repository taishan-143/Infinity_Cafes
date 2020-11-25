import unittest
from unittest import mock
from transform_lambda import handler

class TransformTests(unittest.TestCase):
    def test_get_timestamp(self):
        # Arrange
        list_of_data = [["21/10/2021 13:22", "a", "b"], ["10/03/2022 08:34", "c", "d"]]
        expected = ["2021/10/21 13:22:00", "2022/03/10 08:34:00"]

        # Act
        actual = handler.get_timestamp(list_of_data)

        # Assert
        self.assertEqual(expected, actual)

    def test_get_first_name_and_surname(self):
        # Arrange
        list_of_data = [["a", "b", "John Doe", "c"], ["d", "e", "Jack Reed", "f"]]
        expected = ["John Doe", "Jack Reed"]

        # Act
        actual = handler.get_first_name_and_surname(list_of_data)

        # Assert
        self.assertEqual(expected, actual)
    
    def test_payment_type(self):
        # Arrange
        list_of_data = [["a", "b", "c", "d", "e", "CASH", "f"], [1, 2, 3, 4, 5, "CARD", 7]]
        expected = ["CASH", "CARD"]

        # Act
        actual = handler.payment_type(list_of_data)

        # Assert
        self.assertEqual(expected, actual)
    
    def test_get_card_number(self):
        list_of_data = [["a", "b", "c", "d", "e", "CASH"], [1, 2, 3, 4, 5, "CARD", "6011133894952871", 6]]
        expected = [0000, 2871]

        actual = handler.get_card_number(list_of_data)

        self.assertEqual(expected, actual)

    def test_list_of_unique_products(self):
        list_of_products = [[" Latte - chocolate - 3.50", "Americano - 2.00", "Espresso - 1.50"], ["Mocha - cinnamon - 2.50", " Americano - 2.00", "Smoothie - banana - 3.00"]]
        expected = ["Latte - chocolate - 3.50", "Americano - 2.00", "Espresso - 1.50", "Mocha - cinnamon - 2.50", "Smoothie - banana - 3.00"]

        actual = handler.list_of_unique_products(list_of_products)

        self.assertEqual(expected, actual)

    def test_append_no_flavour_to_drinks_with_no_flavour(self):
        list_of_drinks = [["Americano", "2.00"], ["Latte", "cinnamon", "3.50"]]
        expected = [["Americano", "No flavour", "2.00"], ["Latte", "cinnamon", "3.50"]]

        actual = handler.append_no_flavour_to_drinks_with_no_flavour(list_of_drinks)

        self.assertEqual(expected, actual)

    def test_strip_spaces_from_product_string(self):
        list_of_orders = [[" Latte - chocolate - 3.50", "Americano - 2.00", "Espresso - 1.50"], ["Mocha - cinnamon - 2.50", " Americano - 2.00", "Smoothie - banana - 3.00"]]
        expected = [["Latte - chocolate - 3.50", "Americano - 2.00", "Espresso - 1.50"], ["Mocha - cinnamon - 2.50", "Americano - 2.00", "Smoothie - banana - 3.00"]]

        actual = handler.strip_spaces_from_product_strings(list_of_orders)

        self.assertEqual(expected, actual)
    
        
    def test_join_drink_attributes_for_orders_table(self):
        list_of_orders = [[["Americano", "No flavour", "2.00"], ["Latte", "cinnamon", "3.50"]], [["Smoothie", "strawberry", "2.50"], ["Mocha", "vanilla", "2.50"]]]
        expected = [["Americano - No flavour - 2.00", "Latte - cinnamon - 3.50"], ["Smoothie - strawberry - 2.50", "Mocha - vanilla - 2.50"]]

        actual = handler.join_drink_attributes_for_orders_table(list_of_orders)

        self.assertEqual(expected, actual)

    @unittest.mock.patch("transform_lambda.handler.hash_ID")
    def test_make_products_by_ID(self, mock_hash):
        mock_hash.return_value = "1"
        list_of_products = [["Americano", "No flavour", "2.00"], ["Latte", "cinnamon", "3.50"]]
        expected = {"1": ["Americano", "No flavour", "2.00"], "1": ["Latte", "cinnamon", "3.50"]}

        actual = handler.make_products_by_id(list_of_products)

        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main() 

