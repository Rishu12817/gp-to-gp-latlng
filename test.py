
# Record the start time
# start_time = datetime.now()
import csv
import os
import sys
sys.path.append("..")
import config
import psycopg2


# print("Start Time for insertion:", start_time.strftime("%H:%M:%S"))
csv_file_path = "reversed_geocoding.csv"
locations_table = "location_clusters_3"
# locations_table = "locations"
# g_p_config=config.gp_config_stag
g_p_config=config.gp_config
# csv_file_path = OUTPUT_FILE_PATH

# Check if the file exists
if not os.path.exists(csv_file_path):
    print(f"Error: The file at {csv_file_path} does not exist.")
    exit()

# Define the expected fields with default values as empty strings
expected_fields = [
    "latitude", "longitude", "Formatted_Address", "Place_ID", "Place_Types", "plus_code",
    "administrative_area_level_2", "political", "administrative_area_level_1", "country", "postal_code",
    "street_number", "route", "sublocality", "sublocality_level_1", "locality", "administrative_area_level_3",
    "premise", "postal_code_suffix", "establishment", "point_of_interest", "neighborhood", "park", "transit_station",
    "landmark", "subpremise", "sublocality_level_2", "campground", "lodging", "bus_station", "subway_station",
    "postal_code_prefix", "sublocality_level_3", "airport", "sublocality_level_4", "train_station",
    "administrative_area_level_4", "administrative_area_level_5", "postal_town", "tourist_attraction", "spa",
    "travel_agency", "light_rail_station", "zoo", "administrative_area_level_7", "administrative_area_level_6",
    "insurance_agency", "post_box", "cafe", "food", "car_rental", "museum", "store"
]

# Establish a connection to the database
try:
    conn = psycopg2.connect(**g_p_config)
    cursor = conn.cursor()
    print(f"Connected to the database successfully.")
    
    # Open the CSV file and read its content
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        # Prepare the SQL statement for insertion using parameterized queries
        insert_query = f"""
        INSERT INTO {locations_table} (
            {", ".join(expected_fields)}
        ) VALUES (
            {", ".join([f"%({field})s" for field in expected_fields])}
        );

        UPDATE {locations_table} SET location_cluster = route;
        """
        
        # Loop through each row in the CSV
        for row in reader:
            # Ensure all expected fields are in the row, assigning "" for any missing field
            complete_row = {field: row.get(field, "") for field in expected_fields}
            
            try:
                # Execute the query with the row's data
                cursor.execute(insert_query, complete_row)
            except Exception as e:
                print(f"Error inserting row: {complete_row}. Error: {e}")
        
        # Commit the transaction
        conn.commit()
        print(f"Data inserted successfully.")



except Exception as e:
    print(f"Error: {e}")
finally:
    # Close the connection
    if conn:
        cursor.close()
        conn.close()
        print(f"Connection closed.")
