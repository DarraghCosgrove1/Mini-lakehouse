#!/usr/bin/env python3
import os
import pandas as pd

SILVER = os.path.join("data", "silver")
GOLD = os.path.join("data", "gold")
os.makedirs(GOLD, exist_ok=True)

def write_parquet(df, name):
    df.to_parquet(os.path.join(GOLD, f"{name}.parquet"), index=False)

if __name__ == "__main__":
    dim_customers = pd.read_parquet(os.path.join(SILVER, "dim_customers.parquet"))
    dim_products  = pd.read_parquet(os.path.join(SILVER, "dim_products.parquet"))
    orders_head   = pd.read_parquet(os.path.join(SILVER, "orders_head.parquet"))
    orders_lines  = pd.read_parquet(os.path.join(SILVER, "orders_lines.parquet"))
    dim_calendar  = pd.read_parquet(os.path.join(SILVER, "dim_calendar.parquet"))
    inv_moves     = pd.read_parquet(os.path.join(SILVER, "inventory_movements.parquet"))
    sensors       = pd.read_parquet(os.path.join(SILVER, "machine_sensor_readings.parquet"))
    downtime      = pd.read_parquet(os.path.join(SILVER, "downtime_events.parquet"))

    # Build order fact
    fact = (orders_lines
            .merge(orders_head[["order_id","order_date","customer_id","status"]], on="order_id", how="left")
            .merge(dim_products[["product_id","unit_price","category"]], on="product_id", how="left"))
    fact["extended_amount"] = fact["quantity"] * fact["unit_price"]
    fact_orders = fact.rename(columns={"order_date":"date"})
    write_parquet(fact_orders, "fact_orders")

    # Dim tables passthrough
    write_parquet(dim_customers, "dim_customers")
    write_parquet(dim_products, "dim_products")
    write_parquet(dim_calendar, "dim_calendar")

    # Inventory snapshot by day/product
    inv_moves["date"] = inv_moves["movement_date"].dt.date
    agg_inv = (inv_moves
        .groupby(["date","product_id","movement_type"], as_index=False)["quantity"]
        .sum())
    write_parquet(agg_inv, "agg_inventory_movements")

    # Machine KPIs by hour
    sensors["hour"] = sensors["timestamp"].dt.floor("H")
    kpi = (sensors.groupby(["hour","machine_id"], as_index=False)
                 .agg(avg_temp=("temp_c","mean"),
                      avg_vibration=("vibration_g","mean"),
                      total_units=("units_per_hour","sum")))
    write_parquet(kpi, "kpi_machine_hourly")

    # Downtime by day/machine
    downtime["date"] = downtime["start_ts"].dt.date
    dt_agg = (downtime.groupby(["date","machine_id"], as_index=False)
                      .agg(events=("event_id","count"),
                           minutes_down=("duration_mins","sum")))
    write_parquet(dt_agg, "agg_downtime_daily")

    print("Silver → Gold complete.")
