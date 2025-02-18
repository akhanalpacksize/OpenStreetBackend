import os
import json
from commons import OSM_json, new_chunk


def process_json_data(input_file=OSM_json, output_folder=new_chunk):
    """
    Processes a JSON file by splitting its items into individual files,
    modifying their content, and saving them with specific naming conventions.
    Args:
        input_file (str): Path to the input JSON file.
        output_folder (str): Folder where the processed files will be saved.
    """
    # Create the folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Open the main JSON file
    if not os.path.isfile(input_file):
        print(f"The file '{input_file}' does not exist or is not accessible.")
        return

    # List to track unprocessed files
    unprocessed_files = []

    with open(input_file, 'r', encoding='utf-8') as f:
        try:
            # Load the JSON data
            data = json.load(f)
            # Process all items in the JSON data
            for item in data:
                try:
                    # Extract relevant fields for file name
                    fanfold_supplier_lat = item.get('Fanfold_Supplier_Latitude__c')
                    fanfold_supplier_lon = item.get('Fanfold_Supplier_Longitude__c')
                    shipping_lat = item.get('ShippingLatitude')
                    shipping_lon = item.get('ShippingLongitude')

                    # Validate coordinates (explicitly check for None, not falsy values)
                    if fanfold_supplier_lat is None or fanfold_supplier_lon is None or shipping_lat is None or shipping_lon is None:
                        print(f"Skipping item due to missing coordinates: {item}")
                        continue

                    # Ensure coordinates are not modified (preserve full precision)
                    fanfold_supplier_lat = str(fanfold_supplier_lat)  # Convert to string without truncation
                    fanfold_supplier_lon = str(fanfold_supplier_lon)
                    shipping_lat = str(shipping_lat)
                    shipping_lon = str(shipping_lon)

                    # Construct the file name using latitude and longitude values
                    file_name = item.get("filename") if item.get(
                        "filename") else f"{fanfold_supplier_lat}_{fanfold_supplier_lon}_{shipping_lat}_{shipping_lon}.json"

                    # Define the full path to save the file inside the output_folder
                    file_path = os.path.join(output_folder, file_name)

                    # Modify the road_distance_raw object
                    if 'road_distance_raw' in item:
                        road_distance_raw = item['road_distance_raw']
                        if road_distance_raw is not None:  # Ensure road_distance_raw is not None
                            # Remove top-level keys
                            road_distance_raw.pop('type', None)
                            road_distance_raw.pop('bbox', None)
                            road_distance_raw.pop('metadata', None)

                            # Remove nested keys from features
                            if 'features' in road_distance_raw:
                                for feature in road_distance_raw['features']:
                                    feature.pop('bbox', None)
                                    feature.pop('type', None)
                                    if 'properties' in feature:
                                        feature['properties'].pop('segments', None)
                                        feature['properties'].pop('way_points', None)
                        else:
                            print(f"Warning: 'road_distance_raw' is None for file: {file_name}")

                    # Remove specified keys after file name construction
                    keys_to_remove = [
                        'Fanfold_Supplier__c',
                        'Fanfold_Supplier_Latitude__c',
                        'Fanfold_Supplier_Longitude__c',
                        'ShippingLatitude',
                        'ShippingLongitude',
                        'ShippingCity',
                        'Distance_from_fanfold_supplier_mi__c'
                    ]
                    for key in keys_to_remove:
                        item.pop(key, None)

                    # Save the modified JSON object to the file
                    with open(file_path, 'w', encoding='utf-8') as file:
                        json.dump(item, file, ensure_ascii=False, indent=4)
                    print(f"Saved: {file_path}")

                except Exception as e:
                    # Log the full JSON content of the unprocessed item
                    unprocessed_files.append(item)  # Store the entire item
                    print(f"Error processing item. File name: {file_name}. Error: {e}")

        except Exception as e:
            print(f"Error loading JSON file: {e}")

    # Print the list of unprocessed files
    if unprocessed_files:
        print("\nUnprocessed Items:")
        for idx, item in enumerate(unprocessed_files, start=1):
            print(f"\nItem {idx}:")
            print(json.dumps(item, ensure_ascii=False, indent=4))  # Pretty-print the JSON content
    else:
        print("All files were processed successfully.")


# Call the function
if __name__ == "__main__":
    process_json_data()
