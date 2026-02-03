#!/usr/bin/env python3
import os
import duckdb

GOLD = os.path.join("data","gold")
DB   = os.path.join(GOLD, "lakehouse.duckdb")

con = duckdb.connect(DB)

def register_parquet_as_tables(con, folder):
    # Create views directly on Parquet
    for fname in os.listdir(folder):
        if fname.endswith(".parquet") and fname != "lakehouse.duckdb":
            table = os.path.splitext(fname)[0]
            path = os.path.join(folder, fname).replace("\\","/")
            con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{path}');")

if __name__ == "__main__":
    register_parquet_as_tables(con, GOLD)
    print("DuckDB built with views over Gold Parquet.")
