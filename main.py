from OSR import fetch_from_ors
from commons import new_chunk
from json_chunk import process_json_data
from upload_to_azure import upload_file_in_chunks, upload_multiple_files

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

        # Replace with the path to your directory
        directory_path = new_chunk  # Replace with the path to your directory containing files
        upload_multiple_files(directory_path)
    except Exception as e:
        print(f"Error during upload_file_in_chunks: {e}")

    print("Pipeline completed successfully!")
