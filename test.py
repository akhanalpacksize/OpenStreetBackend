import os
import pandas as pd
from commons import chunk_folder, OSM_json
from config.env import ORS_URL, API_KEY_OSM
from utils import make_request

def generate_file_name(fanfold_supplier_lat, fanfold_supplier_lon, shipping_lat, shipping_lon):
    """
    Generate the file name in the format:
    f"{fanfold_supplier_lat}_{fanfold_supplier_lon}_{shipping_lat}_{shipping_lon}.json"
    """
    # Convert coordinates to strings to preserve full precision
    return f"{str(fanfold_supplier_lat)}_{str(fanfold_supplier_lon)}_{str(shipping_lat)}_{str(shipping_lon)}.json"

def find_missing_files(csv_file_path, directory_path):
    """
    Read the CSV file, check for missing files, and return a list of missing rows.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    print(df)

    # Check if DataFrame is empty
    if df.empty:
        print("The CSV file is empty.")
        return []

    # Select only the required columns
    required_columns = [
        'Fanfold_Supplier__c',
        'Fanfold_Supplier_Latitude__c',
        'Fanfold_Supplier_Longitude__c',
        'ShippingLatitude',
        'ShippingLongitude',
        'ShippingCity',
        'Distance_from_fanfold_supplier_mi__c'
    ]
    df = df[required_columns]

    # List to store missing rows
    missing_rows = []

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        try:
            # Extract latitude and longitude values from the row
            fanfold_supplier_lat = row['Fanfold_Supplier_Latitude__c']
            fanfold_supplier_lon = row['Fanfold_Supplier_Longitude__c']
            shipping_lat = row['ShippingLatitude']
            shipping_lon = row['ShippingLongitude']

            # Validate coordinates (explicitly check for NaN, not falsy values)
            if pd.isna(fanfold_supplier_lat) or pd.isna(fanfold_supplier_lon) or pd.isna(shipping_lat) or pd.isna(shipping_lon):
                print(f"Skipping row {index} due to missing coordinates")
                continue

            # Generate the file name
            file_name = generate_file_name(fanfold_supplier_lat, fanfold_supplier_lon, shipping_lat, shipping_lon)
            file_path = os.path.join(directory_path, file_name)

            # Check if the file exists
            if os.path.exists(file_path):
                print(f"File already exists: {file_path}")
            else:
                print(f"File does not exist: {file_path}")
                # Add the row to the missing rows list
                missing_rows.append(row)
        except Exception as e:
            print(f"Error processing row {index}: {str(e)}")
            continue
    print(missing_rows)

    return missing_rows

def fetch_from_ors(missing_rows):
    """
    Fetch data for missing rows using OpenRouteService API and save results to JSON.
    """
    # Create a DataFrame from the missing rows
    missing_df = pd.DataFrame(missing_rows)

    # Add a new column for road distance data
    missing_df['road_distance_raw'] = None

    # Process each row and fetch road distance
    for index, row in missing_df.iterrows():
        try:
            # Extract coordinates
            start_lat = row['Fanfold_Supplier_Latitude__c']
            start_lon = row['Fanfold_Supplier_Longitude__c']
            end_lat = row['ShippingLatitude']
            end_lon = row['ShippingLongitude']

            # Ensure coordinates are converted to strings to preserve full precision
            start_lat = str(start_lat)
            start_lon = str(start_lon)
            end_lat = str(end_lat)
            end_lon = str(end_lon)

            # Build the API URL
            url = f"{ORS_URL}?api_key={API_KEY_OSM}&start={start_lon},{start_lat}&end={end_lon},{end_lat}"
            print(url)

            # Make the API request using make_request
            data, headers = make_request(url, method='GET')
            if data:
                # Save the raw JSON response
                missing_df.at[index, 'road_distance_raw'] = data
            else:
                print(f"Failed to fetch road distance for row {index}")
        except Exception as e:
            print(f"Error processing row {index}: {str(e)}")
            continue

    # Save the updated DataFrame to JSON
    missing_df.to_json(OSM_json, orient='records')
    print(f"Data saved to {OSM_json}")

# Example usage
if __name__ == "__main__":
    # Specify the path to the CSV file
    csv_file_path = 'test.csv'  # Replace with the actual CSV file path

    # Specify the directory where the files are located
    directory_path = chunk_folder  # Replace with the actual folder path

    # Find missing files
    missing_rows = find_missing_files(csv_file_path, directory_path)

    # Log the missing rows for debugging
    print("\nMissing Rows:")
    for row in missing_rows:
        print(row)

    # If there are missing rows, fetch their data
    if missing_rows:
        print(f"\nFound {len(missing_rows)} missing files. Fetching data...")
        fetch_from_ors(missing_rows)
    else:
        print("All files exist. No data to fetch.")