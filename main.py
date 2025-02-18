from OSR import fetch_from_ors
from json_chunk import process_json_data
from upload_to_azure import upload_file_in_chunks

if __name__ == '__main__':
    try:
        print("Fetching data from ORS...")
        fetch_from_ors()
    except Exception as e:
        print(f"Error during fetch_from_ors: {e}")
        exit(1)

    try:
        print("Processing JSON data...")
        process_json_data()
    except Exception as e:
        print(f"Error during process_json_data: {e}")
        exit(1)

    try:
        print("Uploading files to Azure...")
        upload_file_in_chunks()
    except Exception as e:
        print(f"Error during upload_file_in_chunks: {e}")
        exit(1)

    print("Pipeline completed successfully!")