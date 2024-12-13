import os
import pandas as pd
import requests
import time
import logging
import psycopg2
import csv
import os
import sys
sys.path.append("..")
import config
from datetime import datetime

# part 1


def get_arg_value(arg):
    try:
        idx = sys.argv.index(arg)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return None

# Get the environment variable from command-line arguments
csv_name = get_arg_value("-c")

# cus_sleep = int(get_arg_value("-s"))

cus_sleep = int(get_arg_value("-s") or 0)


# time.sleep(cus_sleep)
# print(cus_sleep , "sleep", cus_sleep.type())
# exit()
# Exit if the parameter is not passed or no value is provided
if not csv_name:
    print("Error: Please pass a CSV file using the -c parameter. Example: python script.py -c input.csv -s for sleep (int) (optional)")
    sys.exit(1)

# Handle the input file path based on the csv_name
INPUT_FILE_PATH = csv_name if csv_name == "input.csv" else f"/tmp/UniqueLatLong4n5/UniqueLatLong4n5.csv/{csv_name}"
OUTPUT_FILE_PATH = f"csv/reversed_geocoding_{csv_name}.csv"
locations_table = "location_clusters_test"
# locations_table = "location_clusters_3"
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{csv_name}_error_log.txt"),
        logging.StreamHandler()
    ]
)
# Record the start time
start_time = datetime.now()
print("Start Time:", start_time.strftime("%H:%M:%S"))

# Constants for API and file paths
API_KEY = os.getenv('GEOCODING_API_KEY', config.google_api_key)  # Replace with your new API key
BASE_URL = 'https://maps.googleapis.com/maps/api/geocode/json'
# Rate limit parameters
REQUESTS_PER_SECOND = 40
DELAY = 1 / REQUESTS_PER_SECOND

# Read input CSV
try:
    df = pd.read_csv(INPUT_FILE_PATH)
    logging.info(f"Loaded {len(df)} rows from {INPUT_FILE_PATH}")
except Exception as e:
    logging.error(f"Failed to read input file: {e}")
    exit(1)


# df = df.head(5)

# print(df)
# print(df.count())

print("Dataframe total to be process in count")
# Function to fetch location data from Google Geocoding API
def get_location_data(lat, lng, api_key, max_retries=1, delay=60):
    params = {'latlng': f'{lat},{lng}', 'key': api_key}
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Status Code: {response.status_code}")  # Log status code
            if data['status'] == 'OK':
                result = data['results'][0]
                formatted_address = result.get('formatted_address', '')
                place_id = result.get('place_id', '')
                place_types = ', '.join(result.get('types', []))
                components = {comp_type: comp['long_name'] for comp in result.get('address_components', []) for comp_type in comp['types']}
                plus_code = result.get('plus_code', {}).get('compound_code', '')
                return formatted_address, place_id, place_types, components, plus_code
        except requests.exceptions.RequestException as e:
            logging.warning(f"API request failed for ({lat}, {lng}) on attempt {attempt + 1}: {e}")
            time.sleep(delay)
    logging.error(f"Max retries exceeded for coordinates ({lat}, {lng})")
    return None, None, None, {}, ''

# Initialize lists to store API results
addresses, place_ids, place_types, address_components_list, plus_codes = [], [], [], [], []

# Process each row in the DataFrame
for idx, row in df.iterrows():
    lat, lng = row['latitude'], row['longitude']
    logging.info(f"Processing row {idx + 1}/{len(df)}: Latitude {lat}, Longitude {lng}")
    print(f"------------------latitude and longitude------------------{lat}---------{lng}-----------------------------")
    address, place_id, types, components, plus_code = get_location_data(lat, lng, API_KEY)
    addresses.append(address)
    place_ids.append(place_id)
    place_types.append(types)
    address_components_list.append(components)
    plus_codes.append(plus_code)

    # Enforce rate limit
    time.sleep(DELAY)

    # Format data for this row
    formatted_data = {
        "latitude": lat,
        "longitude": lng,
        "Formatted_Address": address,
        "Place_ID": place_id,
        "Place_Types": types,
        "plus_code": plus_code,
        "administrative_area_level_2": components.get('administrative_area_level_2', ''),
        "political": components.get('political', ''),
        "administrative_area_level_1": components.get('administrative_area_level_1', ''),
        "country": components.get('country', ''),
        "postal_code": components.get('postal_code', ''),
        "street_number": components.get('street_number', ''),
        "route": components.get('route', ''),
        "sublocality": components.get('sublocality', ''),
        "sublocality_level_1": components.get('sublocality_level_1', ''),
        "locality": components.get('locality', ''),
        "administrative_area_level_3": components.get('administrative_area_level_3', ''),
        "premise": components.get('premise', ''),
        "postal_code_suffix": components.get('postal_code_suffix', ''),
        "establishment": components.get('establishment', ''),
        "point_of_interest": components.get('point_of_interest', ''),
        "neighborhood": components.get('neighborhood', ''),
        "park": components.get('park', ''),
        "transit_station": components.get('transit_station', ''),
        "landmark": components.get('landmark', ''),
        "subpremise": components.get('subpremise', ''),
        "sublocality_level_2": components.get('sublocality_level_2', ''),
        "campground": components.get('campground', ''),
        "lodging": components.get('lodging', ''),
        "bus_station": components.get('bus_station', ''),
        "subway_station": components.get('subway_station', ''),
        "postal_code_prefix": components.get('postal_code_prefix', ''),
        "sublocality_level_3": components.get('sublocality_level_3', ''),
        "airport": components.get('airport', ''),
        "sublocality_level_4": components.get('sublocality_level_4', ''),
        "train_station": components.get('train_station', ''),
        "administrative_area_level_4": components.get('administrative_area_level_4', ''),
        "administrative_area_level_5": components.get('administrative_area_level_5', ''),
        "postal_town": components.get('postal_town', ''),
        "tourist_attraction": components.get('tourist_attraction', ''),
        "spa": components.get('spa', ''),
        "travel_agency": components.get('travel_agency', ''),
        "light_rail_station": components.get('light_rail_station', ''),
        "zoo": components.get('zoo', ''),
        "administrative_area_level_7": components.get('administrative_area_level_7', ''),
        "administrative_area_level_6": components.get('administrative_area_level_6', ''),
        "insurance_agency": components.get('insurance_agency', ''),
        "post_box": components.get('post_box', ''),
        "cafe": components.get('cafe', ''),
        "food": components.get('food', ''),
        "car_rental": components.get('car_rental', ''),
        "museum": components.get('museum', ''),
        "store": components.get('store', '')
    }

    # Save formatted data to CSV immediately after each hit
    try:
        formatted_df = pd.DataFrame([formatted_data])
        if idx == 0:
            formatted_df.to_csv(OUTPUT_FILE_PATH, mode='w', header=True, index=False)
        else:
            formatted_df.to_csv(OUTPUT_FILE_PATH, mode='a', header=False, index=False)
        logging.info(f"Row {idx + 1} data saved to {OUTPUT_FILE_PATH}")
    except Exception as e:
        logging.error(f"Failed to save data for row {idx + 1}: {e}")
    time.sleep(cus_sleep)

# End of script
end_time = datetime.now()
print("End Time:", end_time.strftime("%H:%M:%S"))

# Calculate total time taken
total_time = end_time - start_time
print("Total Time Taken:", total_time)






# part 2 

# Record the start time
start_time = datetime.now()
print("Start Time for insertion:", start_time.strftime("%H:%M:%S"))
csv_file_path = OUTPUT_FILE_PATH

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
    conn = psycopg2.connect(**config.gp_config_stag)
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

end_time = datetime.now()
print("End Time for the insertion :", end_time.strftime("%H:%M:%S"))

# Calculate total time taken
total_time = end_time - start_time
print("Total Time Taken for insertion:", total_time)
