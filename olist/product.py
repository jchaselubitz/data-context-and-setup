# TO CONTENT CREATORS: MIRROR UPDATES OF THIS FILE INTO
# -`olist/product.py`
# - `olist/product_updated.py`
import pandas as pd
import numpy as np
from olist.data import Olist
from olist.order import Order


class Product:
    def __init__(self):
        # Import data only once
        olist = Olist()
        self.data = olist.get_data()
        self.order = Order()

    def get_product_features(self):
        """
         Returns a DataFrame with:
        'product_id', 'product_category_name', 'product_name_length',
        'product_description_length', 'product_photos_qty', 'product_weight_g',
        'product_length_cm', 'product_height_cm', 'product_width_cm'
        """
        products = self.data["products"]

        # (optional) convert name to English
        en_category = self.data["product_category_name_translation"]
        df = products.merge(en_category, on="product_category_name")
        df.drop(["product_category_name"], axis=1, inplace=True)
        df.rename(
            columns={
                "product_category_name_english": "category",
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length",
            },
            inplace=True,
        )

        return df

    def get_price(self):
        """
        Return a DataFrame with:
        'product_id', 'price'
        """
        order_items = self.data["order_items"]
        # There are many different order_items per product_id, each with different prices. Take the mean of the various prices
        return order_items[["product_id", "price"]].groupby("product_id").mean()

    def get_wait_time(self):
        """
        Returns a DataFrame with:
        'product_id', 'wait_time'
        """
        orders_wait_time = self.order.get_wait_time()
        orders_products = self.data["order_items"][
            ["order_id", "product_id"]
        ].drop_duplicates()
        orders_products_with_time = orders_products.merge(
            orders_wait_time, on="order_id"
        )

        return orders_products_with_time.groupby("product_id", as_index=False).agg(
            {"wait_time": "mean"}
        )

    def get_quantity(self):
        """
        Returns a DataFrame with:
        'product_id', 'n_orders', 'quantity'
        """
        order_items = self.data["order_items"]

        n_orders = order_items.groupby("product_id")["order_id"].nunique().reset_index()
        n_orders.columns = ["product_id", "n_orders"]

        quantity = order_items.groupby("product_id", as_index=False).agg(
            {"order_id": "count"}
        )
        quantity.columns = ["product_id", "quantity"]

        return n_orders.merge(quantity, on="product_id")

    def get_sales(self):
        """
        Returns a DataFrame with:
        'product_id', 'sales'
        """
        return (
            self.data["order_items"][["product_id", "price"]]
            .groupby("product_id")
            .sum()
            .rename(columns={"price": "sales"})
        )

    def get_review_score(self):
        """
        Returns a DataFrame with:
        'product_id', 'share_of_five_stars', 'share_of_one_stars',
        'review_score'
        """
        orders_reviews = self.order.get_review_score()
        orders_products = self.data["order_items"][
            ["order_id", "product_id"]
        ].drop_duplicates()
        df = orders_products.merge(orders_reviews, on="order_id")

        df.groupby("order_id")["product_id"].count().reset_index()["product_id"]

        df["product_count"] = (
            df.groupby("order_id")["product_id"].count().reset_index()["product_id"]
        )

        df["total_cost_of_review"] = df.review_score.map(
            {1: 100, 2: 50, 3: 40, 4: 0, 5: 0}
        )

        df["spread_cost_of_review"] = df["total_cost_of_review"] / df["product_count"]

        df = df.groupby("product_id", as_index=False).agg(
            {
                "dim_is_one_star": "mean",
                "dim_is_five_star": "mean",
                "review_score": "mean",
                "spread_cost_of_review": "sum",
                "total_cost_of_review": "sum",
            }
        )
        df.columns = [
            "product_id",
            "share_of_one_stars",
            "share_of_five_stars",
            "review_score",
            "spread_cost_of_review",
            "total_cost_of_review",
        ]

        return df

    def get_training_data(self, spread_review_penalty=False):
        """
        Returns a DataFrame with:
        ['product_id', 'product_name_length', 'product_description_length',
        'product_photos_qty', 'product_weight_g', 'product_length_cm',
        'product_height_cm', 'product_width_cm', 'category', 'wait_time',
        'price', 'share_of_one_stars', 'share_of_five_stars', 'review_score',
         "total_cost_of_review",
            "spread_cost_of_review", 'n_orders', 'quantity', 'sales', 'revenues',
        'profits']
        """
        training_set = (
            self.get_product_features()
            .merge(self.get_wait_time(), on="product_id")
            .merge(self.get_price(), on="product_id")
            .merge(self.get_review_score(), on="product_id")
            .merge(self.get_quantity(), on="product_id")
            .merge(self.get_sales(), on="product_id")
        )

        # compute the economics (revenues, profits)

        cost_of_reviews = (
            training_set["spread_cost_of_review"]
            if spread_review_penalty
            else training_set["total_cost_of_review"]
        )

        olist_sales_cut = 0.1
        training_set["revenues"] = olist_sales_cut * training_set["sales"]
        training_set["profits"] = training_set["revenues"] - cost_of_reviews
        return training_set

    def get_product_cat(self, spread_review_penalty=False, agg="mean"):
        """
        Returns a DataFrame with `category` as index, and aggregating various properties for each category in columns such as:
        - `quantity`: total number of products sold for this category.
        - `product_weight_g`: mean or median weight per category
        - ...
        """
        products = self.get_training_data(spread_review_penalty)

        columns = list(products.select_dtypes(exclude=["object"]).columns)
        agg_params = dict(zip(columns, [agg] * len(columns)))
        agg_params["quantity"] = "sum"

        product_cat = products.groupby("category").agg(agg_params)
        return product_cat
