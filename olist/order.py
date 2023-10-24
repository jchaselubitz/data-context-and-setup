import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    """
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    """

    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """

        orders = self.data["orders"].copy()

        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        one_day = np.timedelta64(24, "h")
        orders.order_delivered_customer_date = pd.to_datetime(
            orders.order_delivered_customer_date
        )
        orders.order_estimated_delivery_date = pd.to_datetime(
            orders.order_estimated_delivery_date
        )
        orders.order_purchase_timestamp = pd.to_datetime(
            orders.order_purchase_timestamp
        )

        orders_to_process = (
            orders[orders.order_status == "delivered"] if is_delivered else orders
        )

        orders_w_wait_time = orders_to_process.copy()
        orders_w_wait_time["wait_time"] = (
            orders_to_process.order_delivered_customer_date
            - orders_to_process.order_purchase_timestamp
        )
        # print(type(orders_w_wait_time["wait_time"][0]))
        orders_w_expected_wait_time = orders_w_wait_time.copy()
        orders_w_expected_wait_time["expected_wait_time"] = (
            orders_w_wait_time.order_delivered_customer_date
            - orders_w_wait_time.order_estimated_delivery_date
        )

        delay_vs_expected = orders_w_expected_wait_time.copy()
        delay_vs_expected["delay_vs_expected"] = (
            orders_w_expected_wait_time.wait_time
            - orders_w_expected_wait_time.expected_wait_time
        ) / one_day

        return delay_vs_expected[
            [
                "order_id",
                "wait_time",
                "expected_wait_time",
                "delay_vs_expected",
                "order_status",
            ]
        ]

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """

        reviews = self.data["order_reviews"].copy()
        reviews["dim_is_five_star"] = reviews["review_score"].apply(
            lambda x: 1 if x > 4 else 0
        )
        reviews["dim_is_one_star"] = reviews["review_score"].apply(
            lambda x: 1 if x < 2 else 0
        )
        return reviews[
            ["order_id", "dim_is_five_star", "dim_is_one_star", "review_score"]
        ]

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        order_items = self.data["order_items"].copy()
        return_order_items = order_items.groupby("order_id")["order_item_id"].count()
        return_order_items_df = return_order_items.reset_index()
        return_order_items_df.columns = ["order_id", "number_of_products"]

        return return_order_items_df

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        orders = self.data["orders"].copy()
        order_items = self.data["order_items"].copy()
        seller_orders = pd.merge(orders, order_items, on="order_id")
        number_sellers = (
            seller_orders.groupby("order_id")["seller_id"].count().reset_index()
        )
        number_sellers.columns = ["order_id", "number_of_sellers"]
        return number_sellers

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """

        price_and_freight = self.data["order_items"].copy()
        price_and_freight.groupby("order_id").sum()
        return (
            price_and_freight.groupby("order_id")
            .sum()[["price", "freight_value"]]
            .reset_index()
        )

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """

    def get_training_data(self, is_delivered=True, with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']

        """
        orders = self.data["orders"].copy()

        a = pd.merge(orders, self.get_wait_time())
        b = pd.merge(a, self.get_review_score())
        c = pd.merge(b, self.get_number_products())
        d = pd.merge(c, self.get_price_and_freight())
        e = pd.merge(d, self.get_number_sellers())
        print(e)

        e[
            [
                "order_id",
                "wait_time",
                "expected_wait_time",
                "delay_vs_expected",
                "order_status",
                "dim_is_five_star",
                "dim_is_one_star",
                "review_score",
                "number_of_products",
                "price",
                "freight_value",
                # "number_sellers",
            ]
        ]


# Hint: make sure to re-use your instance methods defined above
