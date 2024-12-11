insert_or_update_query = f'''
DO $$
BEGIN
-- Check if the row with the given latitude and longitude exists
IF EXISTS (SELECT 1 FROM location_clusters_test WHERE latitude = '{latitude}' AND longitude = '{longitude}') THEN
    -- If it exists, update the row
    UPDATE location_clusters_test
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
    INSERT INTO location_clusters_test (
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