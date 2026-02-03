#!/usr/bin/env python3
import os
import random
import string
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd

BRONZE_DIR = os.path.join("data", "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)
rng = np.random.default_rng(42)
random.seed(42)

def random_name():
    first = ["Aoife","Jack","Grace","James","Emily","Conor","Sophie","Daniel","Amelia","Adam",
             "Liam","Emma","Olivia","Noah","Mia","Ethan","Ella","Harry","Chloe","Michael"]
    last = ["Murphy","Kelly","Byrne","Ryan","O'Brien","Walsh","O'Sullivan","Doyle","McCarthy","Gallagher",
            "Cosgrove","Healy","Cullen","Conlisk","O'Connor","Nolan","Brennan","Daly","Dempsey","Fitzgerald"]
    return f"{random.choice(first)} {random.choice(last)}"

def random_email(name):
    domain = random.choice(["example.com","mail.com","test.org"])
    handle = name.lower().replace(" ", ".")
    return f"{handle}@{domain}"

def random_country():
    return random.choice(["Ireland","UK","Germany","France","Spain","USA"])

def make_customers(n=1000):
    cust_id = np.arange(1, n+1)
    names = [random_name() for _ in cust_id]
    emails = [random_email(nm) for nm in names]
    countries = [random_country() for _ in cust_id]
    created = [date(2022,1,1) + timedelta(days=int(rng.integers(0, 365*3))) for _ in cust_id]
    df = pd.DataFrame({
        "customer_id": cust_id,
        "customer_name": names,
        "email": emails,
        "country": countries,
        "created_date": pd.to_datetime(created)
    })
    df.to_csv(os.path.join(BRONZE_DIR, "customers.csv"), index=False)

def make_products(n=200):
    prod_id = np.arange(1, n+1)
    cat = ["Sensors","Transceivers","Batteries","Cables","Controllers","Test Kits"]
    categories = [random.choice(cat) for _ in prod_id]
    unit_price = np.round(rng.uniform(5, 500, size=n), 2)
    sku = [f"SKU-{i:05d}" for i in prod_id]
    names = [f"{categories[i-1]} {i}" for i in prod_id]
    df = pd.DataFrame({
        "product_id": prod_id,
        "sku": sku,
        "product_name": names,
        "category": categories,
        "unit_price": unit_price
    })
    df.to_csv(os.path.join(BRONZE_DIR, "products.csv"), index=False)

def make_orders(n_orders=5000, max_lines=5):
    # Generate orders header + line items
    cust = pd.read_csv(os.path.join(BRONZE_DIR, "customers.csv"))
    prod = pd.read_csv(os.path.join(BRONZE_DIR, "products.csv"))
    order_ids = np.arange(1, n_orders+1)
    order_dates = [datetime(2023,1,1) + timedelta(days=int(rng.integers(0, 365*2))) for _ in order_ids]
    customer_ids = rng.choice(cust["customer_id"].values, size=n_orders, replace=True)
    statuses = rng.choice(["PENDING","SHIPPED","DELIVERED","CANCELLED"], size=n_orders, p=[0.1,0.3,0.55,0.05])
    df_orders = pd.DataFrame({
        "order_id": order_ids,
        "order_date": pd.to_datetime(order_dates),
        "customer_id": customer_ids,
        "status": statuses
    })
    df_orders.to_csv(os.path.join(BRONZE_DIR, "orders.csv"), index=False)

    # Lines
    rows = []
    line_id = 1
    for oid in order_ids:
        lines = rng.integers(1, max_lines+1)
        for _ in range(lines):
            pid = int(rng.choice(prod["product_id"].values))
            qty = int(rng.integers(1, 10))
            rows.append([line_id, oid, pid, qty])
            line_id += 1
    df_lines = pd.DataFrame(rows, columns=["order_line_id","order_id","product_id","quantity"])
    df_lines.to_csv(os.path.join(BRONZE_DIR, "order_lines.csv"), index=False)

def make_inventory_movements(n=10000):
    prod = pd.read_csv(os.path.join(BRONZE_DIR, "products.csv"))
    product_ids = prod["product_id"].values
    rows = []
    for i in range(n):
        pid = int(rng.choice(product_ids))
        mv_date = datetime(2023,1,1) + timedelta(days=int(rng.integers(0, 365*2)))
        mv_type = rng.choice(["INBOUND","OUTBOUND"])
        qty = int(rng.integers(1, 100))
        rows.append([i+1, pid, mv_date, mv_type, qty])
    df = pd.DataFrame(rows, columns=["movement_id","product_id","movement_date","movement_type","quantity"])
    df.to_csv(os.path.join(BRONZE_DIR, "inventory_movements.csv"), index=False)

def make_machine_sensors(n=20000, machines=5):
    # Simulate time-series sensor data for industrial vibe
    rows = []
    base = datetime(2024, 1, 1, 0, 0, 0)
    for i in range(n):
        ts = base + timedelta(minutes=int(i))
        machine_id = f"M{(i % machines)+1}"
        temp = round(20 + 5*np.sin(i/200) + rng.normal(0,0.8), 2)
        vib = round(0.5 + 0.1*np.sin(i/50) + rng.normal(0,0.05), 3)
        throughput = int(max(0, rng.normal(48, 7)))
        rows.append([i+1, ts, machine_id, temp, vib, throughput])
    df = pd.DataFrame(rows, columns=["reading_id","timestamp","machine_id","temp_c","vibration_g","units_per_hour"])
    df.to_csv(os.path.join(BRONZE_DIR, "machine_sensor_readings.csv"), index=False)

def make_downtime_events(n=800, machines=5):
    rows = []
    for i in range(n):
        start = datetime(2024,1,1) + timedelta(hours=int(rng.integers(0, 24*180)))
        duration = int(np.clip(rng.normal(45, 20), 5, 240))  # mins
        end = start + timedelta(minutes=duration)
        machine_id = f"M{(i % machines)+1}"
        reason = rng.choice(["Jam","Blocked","Power","Changeover","Quality","Unknown"], p=[0.25,0.2,0.15,0.15,0.15,0.1])
        rows.append([i+1, machine_id, start, end, duration, reason])
    df = pd.DataFrame(rows, columns=["event_id","machine_id","start_ts","end_ts","duration_mins","reason"])
    df.to_csv(os.path.join(BRONZE_DIR, "downtime_events.csv"), index=False)

def make_calendar(start="2023-01-01", end="2025-12-31"):
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"date": dates})
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["dow"] = df["date"].dt.day_name()
    df["is_weekend"] = df["dow"].isin(["Saturday","Sunday"])
    df.to_csv(os.path.join(BRONZE_DIR, "calendar.csv"), index=False)

if __name__ == "__main__":
    make_customers(1000)
    make_products(200)
    make_orders(5000, max_lines=5)
    make_inventory_movements(10000)
    make_machine_sensors(20000, machines=6)
    make_downtime_events(800, machines=6)
    make_calendar()
    print("Synthetic datasets generated in data/bronze")
