import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.append("..")
import config

# Hardcoding for debugging purposes
start_of_previous_day = '2023-12-08 00:00:00'
end_of_previous_day = '2023-12-09 00:00:00'

print(f"Start of Previous Day: {start_of_previous_day}")
print(f"End of Previous Day: {end_of_previous_day}")

# Construct the query using f-strings
query = f"""
    SELECT latitude, longitude, process_end_time
    FROM hhg_merged_data
    WHERE process_end_time >= '{start_of_previous_day}'::timestamp 
      AND process_end_time < '{end_of_previous_day}'::timestamp;
"""

try:
    # Connect to the database
    connection = psycopg2.connect(**config.gp_config_stag)
    cursor = connection.cursor()

    # Print debug information
    print(f"Executing Query: {query}")

    # Execute the query
    cursor.execute(query)
    rows = cursor.fetchall()

    if rows:
        df = pd.DataFrame(rows, columns=['Latitude', 'Longitude', 'Process End Time'])
        
    else:
        print(f"No data found for the range {start_of_previous_day} to {end_of_previous_day}.")

except Exception as error:
    print(f"Error fetching data: {error}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()

print("this isn the ",df)