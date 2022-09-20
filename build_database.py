import os
import requests
import sqlite3
import json
import string
import pandas as pd
from datetime import datetime


def create_index(df: pd.DataFrame, index_name: str) -> pd.DataFrame:
    """Create new index (incremental, starting from 1) for pandas dataframe

    Args:
        df (pd.DataFrame): Pandas dataframe
        index_name (str): Name of new index

    Returns:
        pd.DataFrame: Pandas dataframe with new index
    """

    df.reset_index(inplace=True, drop=True)
    df.index += 1
    df.index.name = index_name
    df.reset_index(inplace=True)

    return df


def get_drinks_data(url: str) -> pd.DataFrame:
    """Pull drinks data from Cocktail DB API

    Args:
        url (str): URL from Cocktail DB API

    Returns:
        pd.DataFrame: Pandas dataframe with drinks data
    """

    response = requests.get(url)
    drinks_data = json.loads(response.text)

    if drinks_data["drinks"] is not None:

        df = pd.json_normalize(drinks_data["drinks"])
        df = df[["idDrink", "strDrink", "strGlass"]].applymap(lambda x: x.lower())
        df.columns = ["cocktail_db_id", "drink_name", "glass_name"]

        return df


def build_drinks_dataframe() -> pd.DataFrame:
    """Build drinks dataframe from Cocktails DB data

    Returns:
        pd.DataFrame: Pandas dataframe with Cocktails DB data
    """

    df_list = []

    for letter in string.ascii_lowercase:
        df = get_drinks_data(
            f"https://www.thecocktaildb.com/api/json/v1/1/search.php?f={letter}"
        )
        if df is not None:
            df_list.append(df)

    df_drinks = pd.concat(df_list)

    df_drinks = create_index(df_drinks, "drink_id")

    return df_drinks


def build_glasses_dataframe(df_drinks) -> pd.DataFrame:
    """Build glasses dataframe from drinks dataframe

    Args:
        df_drinks (_type_): Pandas dataframe with Cocktails DB data

    Returns:
        pd.DataFrame: Pandas dataframe with glasses data
    """
    df_glasses = df_drinks[["glass_name"]].copy()
    df_glasses.drop_duplicates(inplace=True)
    df_glasses.sort_values(by="glass_name", inplace=True)

    df_glasses = create_index(df_glasses, "glass_id")

    return df_glasses


def build_bars_dataframe() -> pd.DataFrame:
    """Build a dataframe with data on bars

    Returns:
        pd.DataFrame: Pandas dataframe with bars data
    """

    data = [[1, "budapest"], [2, "new york"], [3, "london"]]
    df_bars = pd.DataFrame(data=data, columns=["bar_id", "location"])

    return df_bars


def build_inventory_dataframe(raw_csv_path, df_bars, df_glasses) -> pd.DataFrame:
    """Build dataframe with current inventory data

    Args:
        raw_csv_path (_type_): Path to CSV with inventory data
        df_bars (_type_): Pandas dataframe with data on bars
        df_glasses (_type_): Pandas dataframe with data on glasses

    Returns:
        pd.DataFrame: Pandas dataframe with inventory data
    """

    df_inventory = pd.read_csv(raw_csv_path)

    df_inventory["stock"] = df_inventory["stock"].apply(lambda x: int(x.split(" ")[0]))
    df_inventory["glass_type"] = df_inventory["glass_type"].apply(
        lambda x: x.replace("coper mug", "copper mug")
    )

    df_inventory = df_inventory.merge(
        df_bars, left_on="bar", right_on="location", how="left"
    )
    df_inventory = df_inventory.merge(
        df_glasses, left_on="glass_type", right_on="glass_name", how="left"
    )

    df_inventory.rename(columns={"stock": "number_in_stock"}, inplace=True)
    df_inventory = create_index(df_inventory, "inventory_id")

    return df_inventory


def clean_raw_orders(
    raw_csv_path: str,
    datetime_format: str,
    location: str,
    header: int = None,
    seperator: str = ",",
) -> pd.DataFrame:
    """Clean raw orders from CSV

    Args:
        raw_csv_path (str): Path to raw CSV file with orders data
        datetime_format (str): Format of datetime data in raw CSV
        location (str): Bar location
        header (int, optional): Row number of starting value. Defaults to None.
        seperator (str, optional): Separator used in raw file. Defaults to ",".

    Returns:
        pd.DataFrame: Clean orders dataframe
    """
    column_names = ["index", "order_datetime", "drink_name", "order_amount"]

    df = pd.read_csv(raw_csv_path, header=header, sep=seperator, names=column_names)
    df.drop(columns="index", axis=1, inplace=True)

    df["order_datetime"] = df["order_datetime"].apply(
        lambda x: datetime.strptime(x, datetime_format).strftime("%Y-%m-%d %H:%M:%S")
    )
    df["drink_name"] = df["drink_name"].apply(lambda x: x.lower())
    df["order_amount"] = df["order_amount"].astype(float)

    df["location"] = location

    return df


def build_orders_dataframe(
    df_budapest: pd.DataFrame, df_london: pd.DataFrame, df_ny: pd.DataFrame
) -> pd.DataFrame:
    """Build orders dataframe from location-specific dataframes

    Args:
        df_budapest (pd.DataFrame): Clean Budapest orders dataframe
        df_london (pd.DataFrame): Clean London orders dataframe
        df_ny (pd.DataFrame): Clean New York orders dataframe

    Returns:
        pd.DataFrame: Clean orders dataframe
    """

    df_orders = pd.concat([df_budapest, df_london, df_ny])
    df_orders = df_orders.merge(
        df_drinks, left_on="drink_name", right_on="drink_name", how="left"
    )
    df_orders = df_orders.merge(
        df_bars, left_on="location", right_on="location", how="left"
    )

    df_orders = create_index(df_orders, "order_id")

    return df_orders


def add_glass_id_to_drinks_dataframe(
    df_drinks: pd.DataFrame, df_glasses: pd.DataFrame
) -> pd.DataFrame:
    """Merge glasses dataframe with drinks dataframe

    Args:
        df_drinks (pd.DataFrame): Drinks dataframe
        df_glasses (pd.DataFrame): Glasses dataframe

    Returns:
        pd.DataFrame: Drinks dataframe with glass id added
    """

    df_drinks = df_drinks.merge(
        df_glasses, left_on="glass_name", right_on="glass_name", how="left"
    )
    df_drinks.drop(columns=["glass_name"], axis=1, inplace=True)

    return df_drinks


def clean_dataframe(df: pd.DataFrame, data_map: dict) -> pd.DataFrame:
    """Use data map to clean pandas dataframe

    Args:
        df (pd.DataFrame): Pandas dataframe
        data_map (dict): Columns and index name

    Returns:
        pd.DataFrame: Dataframe with specified columns and index
    """
    columns = list(data_map["datatypes"].keys())
    df = df.copy()[columns]

    for column in columns:
        datatype = data_map["datatypes"][column]
        df[column] = df[column].astype(datatype)

    return df


if __name__ == "__main__":

    # Build raw dataframes
    CURRENT_PATH = os.getcwd()
    INVENTORY_CSV_PATH = os.path.join(
        *[CURRENT_PATH, "data", "inventory", "bar_data.csv"]
    )
    BUDAPEST_ORDERS_CSV_PATH = os.path.join(
        *[CURRENT_PATH, "data", "orders", "budapest.csv"]
    )
    LONDON_ORDERS_CSV_PATH = os.path.join(
        *[CURRENT_PATH, "data", "orders", "london_transactions.csv"]
    )
    NEW_YORK_ORDERS_CSV_PATH = os.path.join(*[CURRENT_PATH, "data", "orders", "ny.csv"])

    DRINKS_MAP = {
        "datatypes": {
            "drink_id": int,
            "drink_name": str,
            "glass_id": int,
            "cocktail_db_id": int,
        },
        "index_column": "drink_id",
    }
    GLASSES_MAP = {
        "datatypes": {"glass_id": int, "glass_name": str},
        "index_column": "glass_id",
    }
    BARS_MAP = {"datatypes": {"bar_id": int, "location": str}, "index_column": "bar_id"}
    INVENTORY_MAP = {
        "datatypes": {
            "inventory_id": int,
            "glass_id": int,
            "number_in_stock": int,
            "bar_id": int,
        },
        "index_column": "inventory_id",
    }
    ORDERS_MAP = {
        "datatypes": {
            "order_id": int,
            "order_datetime": str,
            "drink_id": int,
            "bar_id": int,
            "order_amount": float,
        },
        "index_column": "order_id",
    }

    # Build dataframes
    df_drinks = build_drinks_dataframe()
    df_glasses = build_glasses_dataframe(df_drinks)
    df_bars = build_bars_dataframe()
    df_inventory = build_inventory_dataframe(INVENTORY_CSV_PATH, df_bars, df_glasses)

    df_budapest = clean_raw_orders(
        raw_csv_path=BUDAPEST_ORDERS_CSV_PATH,
        datetime_format="%Y-%m-%d %H:%M:%S",
        location="budapest",
        header=0,
    )
    df_london = clean_raw_orders(
        raw_csv_path=LONDON_ORDERS_CSV_PATH,
        datetime_format="%Y-%m-%d %H:%M:%S",
        location="london",
        seperator="\t",
    )
    df_ny = clean_raw_orders(
        raw_csv_path=NEW_YORK_ORDERS_CSV_PATH,
        datetime_format="%m-%d-%Y %H:%M",
        location="new york",
        header=1,
    )

    df_orders = build_orders_dataframe(df_budapest, df_london, df_ny)
    df_drinks = add_glass_id_to_drinks_dataframe(df_drinks, df_glasses)

    # Clean dataframes
    df_drinks = clean_dataframe(df_drinks, DRINKS_MAP)
    df_orders = clean_dataframe(df_orders, ORDERS_MAP)
    df_glasses = clean_dataframe(df_glasses, GLASSES_MAP)
    df_inventory = clean_dataframe(df_inventory, INVENTORY_MAP)
    df_bars = clean_dataframe(df_bars, BARS_MAP)

    # Build database
    with open("data_tables.sql") as f:
        data_tables_query = f.read()

    with open("poc_tables.sql") as f:
        poc_tables_query = f.read()

    with sqlite3.connect("bar_database.db") as connection:

        c = connection.cursor()
        c.executescript(data_tables_query)

        df_glasses.to_sql("glasses", connection, if_exists="append", index=False)
        df_bars.to_sql("bars", connection, if_exists="append", index=False)
        df_drinks.to_sql("drinks", connection, if_exists="append", index=False)
        df_inventory.to_sql("inventory", connection, if_exists="append", index=False)
        df_orders.to_sql("orders", connection, if_exists="append", index=False)

        c.executescript(poc_tables_query)
