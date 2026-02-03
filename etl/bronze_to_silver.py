#!/usr/bin/env python3
import os
import pandas as pd

BRONZE = os.path.join("data", "bronze")
SILVER = os.path.join("data", "silver")
os.makedirs(SILVER, exist_ok=True)

def to_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path, index=False)

def clean_customers():
    df = pd.read_csv(os.path.join(BRONZE, "customers.csv"), parse_dates=["created_date"])
    df["email"] = df["email"].str.lower().str.strip()
    df["country"] = df["country"].str.title().str.strip()
    to_parquet(df, os.path.join(SILVER, "dim_customers.parquet"))

def clean_products():
    df = pd.read_csv(os.path.join(BRONZE, "products.csv"))
    df["category"] = df["category"].astype("category")
    df["unit_price"] = pd.to_numeric(df["unit_price"])
    to_parquet(df, os.path.join(SILVER, "dim_products.parquet"))

def clean_orders():
    orders = pd.read_csv(os.path.join(BRONZE, "orders.csv"), parse_dates=["order_date"])
    lines = pd.read_csv(os.path.join(BRONZE, "order_lines.csv"))
    # Filter invalid refs
    orders = orders.dropna(subset=["customer_id"])
    lines = lines.dropna(subset=["order_id","product_id","quantity"])
    # Basic sanity
    lines = lines[lines["quantity"] > 0]
    to_parquet(orders, os.path.join(SILVER, "orders_head.parquet"))
    to_parquet(lines, os.path.join(SILVER, "orders_lines.parquet"))

def clean_inventory():
    df = pd.read_csv(os.path.join(BRONZE, "inventory_movements.csv"), parse_dates=["movement_date"])
    df = df[df["quantity"] > 0]
    df["movement_type"] = df["movement_type"].astype("category")
    to_parquet(df, os.path.join(SILVER, "inventory_movements.parquet"))

def clean_machines():
    sensors = pd.read_csv(os.path.join(BRONZE, "machine_sensor_readings.csv"), parse_dates=["timestamp"])
    down = pd.read_csv(os.path.join(BRONZE, "downtime_events.csv"), parse_dates=["start_ts","end_ts"])
    sensors = sensors.dropna(subset=["temp_c","vibration_g","units_per_hour"])
    down = down[down["duration_mins"] > 0]
    to_parquet(sensors, os.path.join(SILVER, "machine_sensor_readings.parquet"))
    to_parquet(down, os.path.join(SILVER, "downtime_events.parquet"))

def clean_calendar():
    df = pd.read_csv(os.path.join(BRONZE, "calendar.csv"), parse_dates=["date"])
    to_parquet(df, os.path.join(SILVER, "dim_calendar.parquet"))

if __name__ == "__main__":
    clean_customers()
    clean_products()
    clean_orders()
    clean_inventory()
    clean_machines()
    clean_calendar()
    print("Bronze → Silver complete.")
``
