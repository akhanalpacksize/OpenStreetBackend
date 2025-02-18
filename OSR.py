import pandas as pd
from create_dir import create_folder_if_does_not_exist
from upload_to_azure import check_blob_exists
from utils import make_request, json_to_dataframe, get_access_token
from config.env import *
from commons import OSM_json, API_RESPONSE_DIR

access_token = get_access_token()
create_folder_if_does_not_exist(API_RESPONSE_DIR)


def fetch_form_source():
    url = f'{BASE_URL}/datasets/query/execute/{SALESFORCE_ID}'
    base_query = "SELECT * FROM table"
    filters = [
        "Fanfold_Supplier_Longitude__c != 0",
        "Fanfold_Supplier_Latitude__c != 0",
        "Owner_Region__c = 'North America'",
        "ShippingLatitude != ''",
        "ShippingLatitude != 0.0",
        "ShippingState != 'Alaska'",
        "Type = 'Paying Customer'",
        "Name != 'Armstrong/AC Products Accessories Line'",
        "Name != 'Armstrong/AC Products EM7 Addendum'"
    ]

    if filters:
        base_query += f" WHERE {' AND '.join(filters)} "
        base_query += " LIMIT 50;"

    # if filters:
    #     base_query += f" WHERE {' AND '.join(filters)}"
    #
    # # Add the LIMIT clause to restrict the number of rows
    # base_query += " LIMIT 20"

    # Fetch access token
    access_token = get_access_token()
    if not access_token:
        print("Failed to fetch access token")
        return

    # Prepare headers with authorization
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # Make the API request
    data, response_headers = make_request(
        url,
        method='POST',
        data={"sql": base_query},
        headers=headers
    )

    if data:
        print("Data fetched successfully")
        data_rows = data.get('rows', [])
        data_columns = data.get('columns', [])
        source_df = pd.DataFrame(data_rows, columns=data_columns)
        source_df.to_csv('test.csv', index=False)

        return source_df


    else:
        print("Failed to fetch data")


def fetch_from_ors():
    """
    Fetch data from Domo, calculate road distances using OpenRouteService API,
    and save the results to a CSV file.
    """
    # Fetch data from Domo
    df = fetch_form_source()

    # Check if DataFrame is empty
    if df.empty:
        print("No data fetched from Domo.")
        return None

    # Create copy of the DataFrame slice to avoid SettingWithCopyWarning
    sf_account_data = df[
        ['Fanfold_Supplier__c',
         'Fanfold_Supplier_Latitude__c',
         'Fanfold_Supplier_Longitude__c',
         'ShippingLatitude',
         'ShippingLongitude',
         'ShippingCity',
         'Distance_from_fanfold_supplier_mi__c']
    ].copy()  # Add .copy() here to fix the warning

    # Add a new column for road distance data
    sf_account_data['road_distance_raw'] = None
    sf_account_data['filename'] = None
    # Process each row and fetch road distance
    for index, row in sf_account_data.iterrows():
        try:
            # Validate coordinates
            start_lat = row['Fanfold_Supplier_Latitude__c']
            start_lon = row['Fanfold_Supplier_Longitude__c']
            end_lat = row['ShippingLatitude']
            end_lon = row['ShippingLongitude']
            blob_name = f"{start_lat}_{start_lon}_{end_lat}_{end_lon}.json"
            sf_account_data.at[index, 'filename'] = blob_name
            if not check_blob_exists(blob_name):
                # Skip rows with missing coordinates
                if None in (start_lat, start_lon, end_lat, end_lon):
                    print(f"Skipping row {index} due to missing coordinates")
                    continue

                # Build the API URL
                url = f"{ORS_URL}?api_key={API_KEY_OSM}&start={start_lon},{start_lat}&end={end_lon},{end_lat}"
                print(url)

                # Make the API request using make_request
                data, headers = make_request(url, method='GET')
                if data:
                    # Save the raw JSON response
                    sf_account_data.at[index, 'road_distance_raw'] = data
                else:
                    print(f"Failed to fetch road distance for row {index}")

        except Exception as e:
            print(f"Error processing row {index}: {str(e)}")
            continue
    # filter not na or not null
    sf_account_data = sf_account_data[
        sf_account_data["road_distance_raw"].notna() | sf_account_data["road_distance_raw"].notnull()]
    sf_account_data.to_json(OSM_json, orient='records')
    print(f"Data saved to {OSM_json}")

    return sf_account_data


# Example usage
if __name__ == "__main__":
    # fetch_form_source()
    fetch_from_ors()
