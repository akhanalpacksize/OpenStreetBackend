import os
from azure.storage.blob import BlobClient
from commons import new_chunk

# Define the base SAS URL for the container
CONTAINER_SAS_URL = 'https://isgeneral.blob.core.windows.net/openstreetmap?sv=2023-01-03&spr=https%2Chttp&si=full-access-policy-osm&sr=c&sig=XnoOWBn%2B1mj3ntS%2Bv3JHKzAzBTL38IfBnM4nO3T9oeI%3D'


def upload_file_in_chunks(file_path, chunk_size=4 * 1024 * 1024):
    """
    Uploads a single file to Azure Blob Storage in chunks.
    Args:
        file_path (str): Path to the file to upload.
        chunk_size (int): Size of each chunk in bytes (default: 4 MB).
    """
    try:
        # Ensure the provided path is a file (not a directory)
        if not os.path.isfile(file_path):
            raise ValueError(f"The provided path '{file_path}' is not a valid file.")

        # Extract the blob name from the file path
        blob_name = os.path.basename(file_path)
        print(f"Uploading file: {blob_name}")

        # Construct the blob-specific SAS URL
        blob_sas_url = f"{CONTAINER_SAS_URL.split('?')[0]}/{blob_name}?{CONTAINER_SAS_URL.split('?')[1]}"

        # Initialize the BlobClient using the blob-specific SAS URL
        blob_client = BlobClient.from_blob_url(blob_sas_url)

        # Open the local file and upload it in chunks
        with open(file_path, 'rb') as data:
            # Get the total size of the file
            file_size = os.path.getsize(file_path)
            print(f"File size: {file_size} bytes")

            # Start the upload session
            block_list = []
            offset = 0
            while offset < file_size:
                # Read a chunk of the file
                chunk = data.read(chunk_size)
                if not chunk:
                    break

                # Generate a unique block ID for the chunk
                block_id = f"{offset:08d}"  # Use a zero-padded offset as the block ID
                block_id_encoded = block_id.encode('utf-8')

                # Upload the chunk as a block
                blob_client.stage_block(block_id_encoded, chunk)
                block_list.append(block_id_encoded)  # Store the block ID

                # Update the offset
                offset += len(chunk)
                print(f"Uploaded chunk: {len(chunk)} bytes (Offset: {offset})")

            # Commit the block list to finalize the upload
            blob_client.commit_block_list(block_list)

        print(f"File '{blob_name}' uploaded successfully to Azure Blob Storage.")

    except Exception as e:
        print(f"An error occurred while uploading the file: {e}")


def upload_multiple_files(directory_path, chunk_size=4 * 1024 * 1024):
    """
    Uploads all files in a directory to Azure Blob Storage.
    Args:
        directory_path (str): Path to the directory containing files to upload.
        chunk_size (int): Size of each chunk in bytes (default: 4 MB).
    """
    try:
        # Ensure the provided path is a directory
        if not os.path.isdir(directory_path):
            raise ValueError(f"The provided path '{directory_path}' is not a valid directory.")

        # Iterate over all files in the directory
        for file_name in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file_name)

            # Skip directories, only process files
            if os.path.isfile(file_path):
                print(f"Processing file: {file_path}")
                upload_file_in_chunks(file_path, chunk_size)
            else:
                print(f"Skipping directory: {file_path}")

    except Exception as e:
        print(f"An error occurred while processing the directory: {e}")


# Example usage
directory_path = new_chunk  # Replace with the path to your directory containing files
upload_multiple_files(directory_path)