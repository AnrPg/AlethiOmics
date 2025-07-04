#!/usr/bin/env python3


import os
import yaml
import sqlalchemy
from sshtunnel import SSHTunnelForwarder
from collections import defaultdict

from etl import discover as discvr
from etl.utils.misc import create_timestamped_filename, print_and_log

# adjust these for your cluster
SSH_HOST     = "devanr.tenant-a9.svc.cluster.local"
SSH_USER     = "anr"
SSH_KEY_PATH = "/home/rodochrousbisbiki/.ssh/id_ed25519"
REMOTE_HOST  = "127.0.0.1"
REMOTE_PORT  = 3306

# Batch configuration
BATCH_SIZE = 5  # Adjust based on memory and performance requirements

tunnel = SSHTunnelForwarder(
    (SSH_HOST, 22),
    ssh_username=SSH_USER,
    ssh_pkey=SSH_KEY_PATH,
    remote_bind_address=(REMOTE_HOST, REMOTE_PORT),
    local_bind_address=('127.0.0.1',)  # let it pick a port
)
tunnel.start()
local_port = tunnel.local_bind_port

# after you have `local_port` from above (or `3307` if you forwarded manually):
USER     = os.getenv("MYSQL_USER", "MasterLogariasmos")
PWD      = os.getenv("MYSQL_PASS", "1234")
DB_NAME  = os.getenv("MYSQL_DB",   "alethiomics_live")
HOST     = "127.0.0.1"             # localhost via tunnel
PORT     = local_port              # or 3307

mysql_url = (
    f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/{DB_NAME}"
    "?charset=utf8mb4"
)
engine = sqlalchemy.create_engine(mysql_url, pool_pre_ping=True)

def load_mapping(path="config/features.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Initialize batch accumulator
batch_staging = defaultdict(set)
batch_num = 1
batch_count = 0

discover_dir = "raw_data"
logfile = create_timestamped_filename("./debug_logs")
print_and_log(f"Looking for files in directory: {discover_dir}\n")

for path in discvr.discover(discover_dir):
    from etl.extract import extract
    from etl.load import load   
    from etl.harmonise import harmonize
    from etl.utils.preprocessing import lowercase_ascii
    
    mapping = load_mapping("config/features.yml")
    for row in extract(path, mapping, mode='metadata'):
        
        # , add_timestamp=False, logfile_path=logfile, collapse_size=3
        # row["value"] = lowercase_ascii(str(row["value"]) if row["value"] is not None else "") 
        harmonised_row = harmonize([row], mapping)
        if not harmonised_row:
            # print_and_log(f"\t!!!!!!!\tNo harmonised data for row: {row}, skipping...\n")
            continue
        
        # print_and_log(f"Loading metadata from file: {path}\n")  
        # Accumulate staging data into batch
        
        # print_and_log(f"\n\nProcessing row: {row}\n")
        # print_and_log(f"Harmonised row:\n")
        # [print_and_log(f"{key}: {value}") for key, value in harmonised_row.items()]
        # print_and_log(f"Harmonised row keys: {list(harmonised_row.keys())}\n")
        # print_and_log(f"Harmonised row values: {list(harmonised_row.values())}\n\n\n")
#         for (table, column), values in harmonised_row.items():
#             batch_staging[(table, column)].update(values)
#             print_and_log(f"Staging datum #{batch_count+1} for table: {table}, column: {column}, values: {values}")
#             batch_count += 1

        
#         # Process batch when it reaches BATCH_SIZE
#         if batch_count >= BATCH_SIZE:
#             print_and_log(f"Processing batch {batch_num} of {batch_count} records")
#             print_and_log(f"Loading batch data into MySQL database: {mysql_url}\n")
            
#             # Convert defaultdict to regular dict for load function
#             staging_dict = dict(batch_staging)
#             load(staging_dict, mysql_url)
            
#             print_and_log(f"Batch completed successfully\n")
            
#             # Reset batch accumulator
#             batch_staging = defaultdict(set)
#             batch_num += 1
#             batch_count = 0

# # Process any remaining records in the final batch
# if batch_count > 0:
#     print_and_log(f"Processing final batch of {batch_count} records")
#     print_and_log(f"Loading batch data into MySQL database: {mysql_url}\n")
    
#     staging_dict = dict(batch_staging)
#     load(staging_dict, mysql_url)
    
#     print_and_log(f"Final batch completed successfully\n")
        
tunnel.stop()
print_and_log("ETL process completed successfully.\n\n")
# Close the database connection
engine.dispose()
# Close the SSH tunnel
tunnel.close()
print_and_log("SSH tunnel closed.")
print_and_log("Database connection disposed.")