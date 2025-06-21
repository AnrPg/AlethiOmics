#!/usr/bin/env python3


import pandas
import sqlalchemy
from sqlalchemy import text

# This loader now supports loading multiple columns into each table,
# with mismatched column-length handling (pads shorter lists with None),
# and detailed debug output at every step.

def load(staging, mysql_url):
    print("[DEBUG] Starting load process")
    print(f"[DEBUG] MySQL URL: {mysql_url}")
    engine = sqlalchemy.create_engine(mysql_url)
    with engine.begin() as conn:
        # Organize staging data by table
        tables: dict[str, dict[str, list]] = {}
        for (table, col), values in staging.items():
            print(f"[DEBUG] Staging entry: table='{table}', column='{col}', {len(values)} values")
            tables.setdefault(table, {})[col] = list(values)

        for table, cols in tables.items():
            print(f"\n[DEBUG] Processing table: '{table}' with columns: {list(cols.keys())}")
            if not cols:
                print(f"[DEBUG] No columns to load for table '{table}' -> skipping")
                continue

            # Handle mismatched column lengths by padding shorter lists with None
            lengths = {col_name: len(vals) for col_name, vals in cols.items()}
            max_len = max(lengths.values())
            print(f"[DEBUG] Column lengths: {lengths}, max length: {max_len}")
            for col_name, vals in list(cols.items()):
                if len(vals) < max_len:
                    pad_count = max_len - len(vals)
                    print(f"[DEBUG] Padding column '{col_name}' with {pad_count} None values")
                    cols[col_name] = vals + [None] * pad_count

            # Build a DataFrame for all columns of this table
            df = pandas.DataFrame(cols)
            print(f"[DEBUG] DataFrame preview for table '{table}':\n{df.head(20)}" )
            if df.empty:
                print(f"[DEBUG] DataFrame is empty for table '{table}' -> skipping")
                continue
            
            # !!! 
            # If you load data less than a few thousand of rows then consider loading
            # with a bulk-parameterized INSERT (like `INSERT INTO table (col) VALUES (%s)`).
            # Temp table is preferable for larger datasets to avoid memory issues, in which case
            # you should uncomment the temp table code below.
            # !!!

            # temp_name = f"temp_{table}"
            # print(f"[DEBUG] Writing staged data to temporary table '{temp_name}'")
            # df.to_sql(temp_name, conn, index=False, if_exists="replace")
            # print(f"----->[DEBUG] staged {df.shape[0]} rows and {df.shape[1]} columns into {temp_name}")

            # Prepare insert statement
            columns = df.columns.tolist()
            col_list = ", ".join(f"`{c}`" for c in columns)
            named_ph = "(" + ", ".join(f":{c}" for c in columns) + ")"
            update_clause = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in columns)

            print(f"""
                INSERT INTO `{table}` ({col_list})
                VALUES {named_ph}
                ON DUPLICATE KEY UPDATE {update_clause};
            """)

            print(f"[DEBUG] Executing INSERT for table '{table}': columns={columns}")
            sql = text(f"""
                INSERT INTO `{table}` ({col_list})
                VALUES {named_ph}
                ON DUPLICATE KEY UPDATE {update_clause}
            """)

            params = df.to_dict(orient="records")
            print(f"\t#########\t[DEBUG] Row records prepared: {params[:5]}... (total {len(params)} records)")
            conn.execute(sql, params)
            print(f"[DEBUG] INSERT completed for table '{table}'")

            # # Clean up temporary table
            # print(f"[DEBUG] Dropping temporary table '{temp_name}'")
            # conn.execute(text(f"DROP TABLE `{temp_name}`;"))
            # print(f"----->[DEBUG] loaded into {table}: {df.shape[0]} rows")

    print("[DEBUG] Load process completed")
