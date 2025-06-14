#!/usr/bin/env python3

import pandas
import sqlalchemy

def load(staging, mysql_url):
    engine = sqlalchemy.create_engine(mysql_url)
    with engine.begin() as conn:
        for (table, col), values in staging.items():
            df = pandas.DataFrame({col: list(values)})
            df.to_sql("#temp", conn, index=False, if_exists="replace")  # temp table
            conn.execute(f"""
                INSERT INTO {table} ({col})
                SELECT {col} FROM #temp
                ON DUPLICATE KEY UPDATE {col}={col};
            """)
