import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.append("..")
import config

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
g_p_config = config.gp_config_stag
# g_p_config = config.gp_config
print(g_p_config)

today = datetime.today()
previous_day = today - timedelta(days=1)

# Format the start and end of the previous day
start_of_previous_day = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_previous_day = previous_day.replace(hour=23, minute=59, second=59, microsecond=999999)
# Hardcoding for debugging purposes
start_of_previous_day = '2023-12-08 00:00:00'
# end_of_previous_day = '2024-12-15 00:00:00'

print(f"Start of Previous Day: {start_of_previous_day}")
print(f"End of Previous Day: {end_of_previous_day}")



merged_data_table = "hhg_merged_data"
# location_clusters_table = "location_clusters_test"
location_clusters_table = "location_clusters"

# Construct the query using f-strings
query = f''' 
            SELECT hhg.latitude,hhg.longitude,hhg.process_end_time 
            FROM {merged_data_table} hhg
            LEFT JOIN {location_clusters_table} lct
                ON hhg.latitude = lct.latitude 
                AND hhg.longitude = lct.longitude
            WHERE
            hhg.process_end_time >= '{start_of_previous_day}'::timestamp
            AND hhg.process_end_time < '{end_of_previous_day}'::timestamp
            AND lct.place_id is Null;
        '''

try:
    # Connect to the database
    connection = psycopg2.connect(**g_p_config)
    cursor = connection.cursor()

    # Print debug information
    print(f"Executing Query: \n\n{query}")

    # Execute the query
    cursor.execute(query)
    rows = cursor.fetchall()

    if rows:
        df = pd.DataFrame(rows, columns=['latitude', 'longitude','process_end_time'])
        
    else:
        print(f"No data found for the range {start_of_previous_day} to {end_of_previous_day}.")
        exit()

except Exception as error:
    print(f"Error fetching data: {error}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()

print("this is the fetched data: \n\n",df)
OUTPUT_FILE_PATH = "reversed_geocoding.csv"
# exit()
# Constants for API and file paths
API_KEY = os.getenv('GEOCODING_API_KEY', config.google_api_key)  # Replace with your new API key
BASE_URL = 'https://maps.googleapis.com/maps/api/geocode/json'
# Rate limit parameters
REQUESTS_PER_SECOND = 40
DELAY = 1 / REQUESTS_PER_SECOND

# df = df.head(1)

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




# part 2 

# Record the start time
start_time = datetime.now()
print("Start Time for insertion:", start_time.strftime("%H:%M:%S"))
csv_file_path = OUTPUT_FILE_PATH




# Check if the file exists
if not os.path.exists(csv_file_path):
    print(f"Error: The file at {csv_file_path} does not exist.")
    exit()


# Define the expected field names
expected_fields = [
    'latitude', 'longitude', 'Formatted_Address', 'Place_ID', 'Place_Types', 'plus_code',
    'administrative_area_level_2', 'political', 'administrative_area_level_1', 'country', 'postal_code',
    'street_number', 'route', 'sublocality', 'sublocality_level_1', 'locality', 'administrative_area_level_3',
    'premise', 'postal_code_suffix', 'establishment', 'point_of_interest', 'neighborhood', 'park',
    'transit_station', 'landmark', 'subpremise', 'sublocality_level_2', 'campground', 'lodging',
    'bus_station', 'subway_station', 'postal_code_prefix', 'sublocality_level_3', 'airport',
    'sublocality_level_4', 'train_station', 'administrative_area_level_4', 'administrative_area_level_5',
    'postal_town', 'tourist_attraction', 'spa', 'travel_agency', 'light_rail_station', 'zoo',
    'administrative_area_level_7', 'administrative_area_level_6', 'insurance_agency', 'post_box',
    'cafe', 'food', 'car_rental', 'museum', 'store'
]

try:
    # Establish a database connection
    conn = psycopg2.connect(**g_p_config)
    cursor = conn.cursor()
    print("Connected to the database successfully.")
    
    # Open the CSV file and read its content
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        # Loop through each row in the CSV
        for row in reader:
            # Assign CSV fields to variables
            latitude = row.get('latitude', '')
            longitude = row.get('longitude', '')
            formatted_address = row.get('Formatted_Address', '').replace("'", "''")
            place_id = row.get('Place_ID', '')
            place_types = row.get('Place_Types', '').replace("'", "''")
            plus_code = row.get('plus_code', '')
            administrative_area_level_2 = row.get('administrative_area_level_2', '').replace("'", "''")
            political = row.get('political', '').replace("'", "''")
            administrative_area_level_1 = row.get('administrative_area_level_1', '').replace("'", "''")
            country = row.get('country', '')
            postal_code = row.get('postal_code', '')
            street_number = row.get('street_number', '')
            route = row.get('route', '').replace("'", "''")
            sublocality = row.get('sublocality', '').replace("'", "''")
            sublocality_level_1 = row.get('sublocality_level_1', '').replace("'", "''")
            locality = row.get('locality', '').replace("'", "''")
            administrative_area_level_3 = row.get('administrative_area_level_3', '').replace("'", "''")
            premise = row.get('premise', '').replace("'", "''")
            postal_code_suffix = row.get('postal_code_suffix', '').replace("'", "''")
            establishment = row.get('establishment', '').replace("'", "''")
            point_of_interest = row.get('point_of_interest', '').replace("'", "''")
            neighborhood = row.get('neighborhood', '').replace("'", "''")
            park = row.get('park', '').replace("'", "''")
            transit_station = row.get('transit_station', '').replace("'", "''")
            landmark = row.get('landmark', '').replace("'", "''")
            subpremise = row.get('subpremise', '').replace("'", "''")
            sublocality_level_2 = row.get('sublocality_level_2', '').replace("'", "''")
            campground = row.get('campground', '').replace("'", "''")
            lodging = row.get('lodging', '').replace("'", "''")
            bus_station = row.get('bus_station', '').replace("'", "''")
            subway_station = row.get('subway_station', '').replace("'", "''")
            postal_code_prefix = row.get('postal_code_prefix', '').replace("'", "''")
            sublocality_level_3 = row.get('sublocality_level_3', '').replace("'", "''")
            airport = row.get('airport', '').replace("'", "''")
            sublocality_level_4 = row.get('sublocality_level_4', '').replace("'", "''")
            train_station = row.get('train_station', '').replace("'", "''")
            administrative_area_level_4 = row.get('administrative_area_level_4', '').replace("'", "''")
            administrative_area_level_5 = row.get('administrative_area_level_5', '').replace("'", "''")
            postal_town = row.get('postal_town', '').replace("'", "''")
            tourist_attraction = row.get('tourist_attraction', '').replace("'", "''")
            spa = row.get('spa', '').replace("'", "''")
            travel_agency = row.get('travel_agency', '').replace("'", "''")
            light_rail_station = row.get('light_rail_station', '').replace("'", "''")
            zoo = row.get('zoo', '').replace("'", "''")
            administrative_area_level_7 = row.get('administrative_area_level_7', '').replace("'", "''")
            administrative_area_level_6 = row.get('administrative_area_level_6', '').replace("'", "''")
            insurance_agency = row.get('insurance_agency', '').replace("'", "''")
            post_box = row.get('post_box', '').replace("'", "''")
            cafe = row.get('cafe', '').replace("'", "''")
            food = row.get('food', '').replace("'", "''")
            car_rental = row.get('car_rental', '').replace("'", "''")
            museum = row.get('museum', '').replace("'", "''")
            store = row.get('store', '').replace("'", "''")
            insert_or_update_query = f'''
            DO $$
            BEGIN
                -- Check if the row with the given latitude and longitude exists
                IF EXISTS (SELECT 1 FROM {location_clusters_table} WHERE latitude = '{latitude}' AND longitude = '{longitude}') THEN
                    -- If it exists, update the row
                    UPDATE {location_clusters_table}
                    SET 
                        location_cluster = '{route}',
                        Formatted_Address = '{formatted_address}',
                        Place_ID = '{place_id}',
                        Place_Types = '{place_types}',
                        plus_code = '{plus_code}',
                        administrative_area_level_2 = '{administrative_area_level_2}', 
                        political = '{political}',
                        administrative_area_level_1 = '{administrative_area_level_1}',
                        country = '{country}', postal_code = '{postal_code}',
                        street_number = '{street_number}', route = '{route}', sublocality = '{sublocality}', 
                        sublocality_level_1 = '{sublocality_level_1}', locality = '{locality}', 
                        administrative_area_level_3 = '{administrative_area_level_3}',
                        premise = '{premise}', postal_code_suffix = '{postal_code_suffix}', 
                        establishment = '{establishment}', point_of_interest = '{point_of_interest}', 
                        neighborhood = '{neighborhood}', park = '{park}', transit_station = '{transit_station}', 
                        landmark = '{landmark}', subpremise = '{subpremise}', sublocality_level_2 = '{sublocality_level_2}', 
                        campground = '{campground}', lodging = '{lodging}', bus_station = '{bus_station}', 
                        subway_station = '{subway_station}', postal_code_prefix = '{postal_code_prefix}',
                        sublocality_level_3 = '{sublocality_level_3}', airport = '{airport}', 
                        sublocality_level_4 = '{sublocality_level_4}', train_station = '{train_station}', 
                        administrative_area_level_4 = '{administrative_area_level_4}', 
                        administrative_area_level_5 = '{administrative_area_level_5}', postal_town = '{postal_town}', 
                        tourist_attraction = '{tourist_attraction}', spa = '{spa}', 
                        travel_agency = '{travel_agency}', light_rail_station = '{light_rail_station}', 
                        zoo = '{zoo}', administrative_area_level_7 = '{administrative_area_level_7}', 
                        administrative_area_level_6 = '{administrative_area_level_6}', insurance_agency = '{insurance_agency}', 
                        post_box = '{post_box}', cafe = '{cafe}', food = '{food}', car_rental = '{car_rental}', 
                        museum = '{museum}', store = '{store}'
                    WHERE latitude = '{latitude}' AND longitude = '{longitude}';
                ELSE
                    -- If it does not exist, insert a new row
                    INSERT INTO {location_clusters_table} (
                        location_cluster, latitude, longitude, Formatted_Address, Place_ID, Place_Types, plus_code,
                        administrative_area_level_2, political, administrative_area_level_1, country, postal_code,
                        street_number, route, sublocality, sublocality_level_1, locality, administrative_area_level_3,
                        premise, postal_code_suffix, establishment, point_of_interest, neighborhood, park,
                        transit_station, landmark, subpremise, sublocality_level_2, campground, lodging,
                        bus_station, subway_station, postal_code_prefix, sublocality_level_3, airport,
                        sublocality_level_4, train_station, administrative_area_level_4, administrative_area_level_5,
                        postal_town, tourist_attraction, spa, travel_agency, light_rail_station, zoo,
                        administrative_area_level_7, administrative_area_level_6, insurance_agency, post_box,
                        cafe, food, car_rental, museum, store
                    ) 
                    VALUES (
                        '{route}', '{latitude}', '{longitude}', '{formatted_address}', '{place_id}', '{place_types}', '{plus_code}',
                        '{administrative_area_level_2}', '{political}', '{administrative_area_level_1}', '{country}', '{postal_code}',
                        '{street_number}', '{route}', '{sublocality}', '{sublocality_level_1}', '{locality}', '{administrative_area_level_3}',
                        '{premise}', '{postal_code_suffix}', '{establishment}', '{point_of_interest}', '{neighborhood}', '{park}',
                        '{transit_station}', '{landmark}', '{subpremise}', '{sublocality_level_2}', '{campground}', '{lodging}',
                        '{bus_station}', '{subway_station}', '{postal_code_prefix}', '{sublocality_level_3}', '{airport}',
                        '{sublocality_level_4}', '{train_station}', '{administrative_area_level_4}', '{administrative_area_level_5}',
                        '{postal_town}', '{tourist_attraction}', '{spa}', '{travel_agency}', '{light_rail_station}', '{zoo}',
                        '{administrative_area_level_7}', '{administrative_area_level_6}', '{insurance_agency}', '{post_box}',
                        '{cafe}', '{food}', '{car_rental}', '{museum}', '{store}'
                    );
                END IF;
            END $$;
            '''


            print("================ start ================")
            print("\nExecuting Query:\n", insert_or_update_query)
            print("================ end ================")
            # Execute the query
            try:
                cursor.execute(insert_or_update_query)
            except Exception as e:
                print(f"Error executing query for row: {row}. Error: {e}")
        
        # Commit the transaction
        conn.commit()
        print("Data inserted successfully.")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    # Close the connection
    if conn:
        cursor.close()
        conn.close()
        print("Connection closed.")

end_time = datetime.now()
print("End Time for the insertion :", end_time.strftime("%H:%M:%S"))

# Calculate total time taken
total_time = end_time - start_time
print("Total Time Taken for insertion:", total_time)
