#!/usr/bin/env python3
import os
import sys
import pandas as pd

SILVER = os.path.join("data","silver")
GOLD   = os.path.join("data","gold")

def assert_non_null(df, cols, table):
    for c in cols:
        if df[c].isna().any():
            raise AssertionError(f"{table}: column '{c}' contains nulls")

def assert_unique(df, cols, table):
    if df.duplicated(subset=cols).any():
        raise AssertionError(f"{table}: duplicates on {cols}")

def main():
    dim_customers = pd.read_parquet(os.path.join(SILVER,"dim_customers.parquet"))
    dim_products  = pd.read_parquet(os.path.join(SILVER,"dim_products.parquet"))
    orders_head   = pd.read_parquet(os.path.join(SILVER,"orders_head.parquet"))
    orders_lines  = pd.read_parquet(os.path.join(SILVER,"orders_lines.parquet"))

    assert_unique(dim_customers, ["customer_id"], "dim_customers")
    assert_unique(dim_products, ["product_id"], "dim_products")
    assert_non_null(orders_head, ["order_id","customer_id","order_date"], "orders_head")
    assert_non_null(orders_lines, ["order_line_id","order_id","product_id","quantity"], "orders_lines")

    fact_orders = pd.read_parquet(os.path.join(GOLD, "fact_orders.parquet"))
    if (fact_orders["extended_amount"] < 0).any():
        raise AssertionError("fact_orders: negative extended_amount")

    print("All DQ checks passed.")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(str(e))
        sys.exit(1)
