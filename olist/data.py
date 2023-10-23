import os
import pandas as pd

"""
This function returns a Python dict.
Its keys should be 'sellers', 'orders', 'order_items' etc...
Its values should be pandas.DataFrames loaded from csv files
"""


class Olist:
    def get_data(self):
        csv_path = os.path.join(os.environ["PYTHONPATH"][:-1], "data", "csv")
        file_names = os.listdir(csv_path)
        data = {}
        for name in file_names:
            if name[:2] != ".k":
                key = (
                    name.replace("olist_", "")
                    .replace(".csv", "")
                    .replace("_dataset", "")
                )

                data[key] = pd.read_csv(os.path.join(csv_path, name))
        return data

    def ping(self):
        print("pong")
