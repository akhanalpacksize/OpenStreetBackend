import json
import time
import logging
import os.path
import smtplib
from base64 import b64encode
from copy import deepcopy
from datetime import datetime
from email.mime.text import MIMEText
import pandas
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from json import JSONDecodeError
from threading import Lock


from config.env import EMAIL, RECIPIENTS, GOOGLE_PASSWORD, GRANT_TYPE, AUTH_URL, CLIENT_ID, CLIENT_SECRET

# from logger_config import setup_logging

# Setup logging
# setup_logging(module_name="Fetch_utils_log")

logger = logging.getLogger(__name__)





# def make_request(url, method='GET', data=None, headers=None, max_retries=3, retry_delay=5):
#     """
#     Make an HTTP request with retries and rate limiting handling.
#
#     Args:
#         url (str): The URL to make the request to.
#         method (str): The HTTP method (GET or POST).
#         data (dict): The JSON payload for POST requests.
#         headers (dict): Additional headers to include in the request.
#         max_retries (int): Maximum number of retries for failed requests.
#         retry_delay (int): Initial delay between retries in seconds.
#
#     Returns:
#         tuple: (response_data, response_headers) if successful, otherwise (None, None).
#     """
#     retries = 0
#
#     # Default headers
#     default_headers = {
#         'Content-Type': 'application/json',
#     }
#
#     # Merge default headers with provided headers
#     if headers:
#         default_headers.update(headers)
#
#     while retries < max_retries:
#         try:
#             # Make the request
#             if method.upper() == 'GET':
#                 response = requests.get(url, headers=default_headers)
#             elif method.upper() == 'POST':
#                 response = requests.post(url, headers=default_headers, json=data)
#             else:
#                 raise ValueError(f'Unsupported method: {method}')
#
#             # Handle response
#             if response.status_code == 200:
#                 # Success
#                 return response.json(), response.headers
#             elif response.status_code == 400:
#                 # Bad request
#                 logger.error(f'Client error: {response.status_code}, {response.text}')
#                 return None, None
#             elif response.status_code == 401:
#                 # Unauthorized
#                 logger.error(f'Unauthorized: {response.status_code}, {response.text}')
#                 return None, None
#             elif response.status_code == 403:
#                 # Forbidden
#                 logger.error(f'Forbidden: {response.status_code}, {response.text}')
#                 return None, None
#             elif response.status_code == 429:
#                 # Rate limited
#                 retry_after = int(response.headers.get('Retry-After', retry_delay))
#                 logger.warning(f'Rate limited. Retrying in {retry_after} seconds...')
#                 time.sleep(retry_after)
#                 retries += 1
#             else:
#                 # Other errors
#                 logger.error(f'Error {response.status_code}: {response.text}')
#                 return None, None
#
#         except requests.exceptions.RequestException as e:
#             # Network or connection error
#             logger.error(f'Request failed: {e}')
#             retries += 1
#             if retries < max_retries:
#                 logger.info(f'Retrying in {retry_delay} seconds...')
#                 time.sleep(retry_delay)
#             else:
#                 return None, None
#
#     # Max retries exceeded
#     logger.error(f'Max retries ({max_retries}) exceeded.')
#     return None, None

class RateLimiter:
    def __init__(self, max_requests, period):
        self.max_requests = max_requests
        self.period = period  # in seconds
        self.request_count = 0
        self.last_reset = datetime.now()
        self.lock = Lock()

    def check_limit(self):
        with self.lock:
            elapsed = (datetime.now() - self.last_reset).total_seconds()
            if elapsed > self.period:
                self.request_count = 0
                self.last_reset = datetime.now()

            if self.request_count >= self.max_requests:
                wait_time = self.period - elapsed
                return max(wait_time, 0)  # Ensure non-negative
            return 0

    def increment(self):
        with self.lock:
            self.request_count += 1


# Initialize rate limiter (40 requests per 60 seconds)
rate_limiter = RateLimiter(40, 60)


def make_request(url, method='GET', params=None, data=None, headers=None, max_retries=3):
    retries = 0
    default_headers = {'Content-Type': 'application/json'}

    if headers:
        default_headers.update(headers)

    while retries < max_retries:
        # Enforce rate limit
        wait_time = rate_limiter.check_limit()
        if wait_time > 0:
            logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        try:
            # Make request
            if method.upper() == 'GET':
                response = requests.get(url, headers=default_headers, params=params)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=default_headers, json=data)
            else:
                raise ValueError(f'Unsupported method: {method}')

            rate_limiter.increment()

            # Handle response
            if response.status_code == 200:
                try:
                    return response.json(), response.headers
                except JSONDecodeError:
                    logger.error("Invalid JSON response")
                    return None, None
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f'Rate limited. Retrying in {retry_after} seconds...')
                time.sleep(retry_after)
                continue  # Do not increment retries
            else:
                logger.error(f'Error {response.status_code}: {response.text}')
                retries += 1

        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed: {e}')
            retries += 1
            if retries < max_retries:
                time.sleep(5)  # Backoff for network errors

    logger.error(f'Max retries ({max_retries}) exceeded.')
    return None, None


def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for index, right_row in enumerate(right):
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data):
    for index, elem in enumerate(data):
        elem['Rank'] = index + 1
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                if value and "\r\n" in str(value):
                    value = value.replace("\r\n", '-')

                if prev_heading:
                    rows = cross_join(rows, flatten_json(value, prev_heading + '_' + key))
                else:
                    rows = cross_join(rows, flatten_json(value, key))

        elif isinstance(data, list):
            rows = []
            for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_json(item, prev_heading))]
        else:
            rows = [{prev_heading: data}]
        return rows

    return pandas.DataFrame(flatten_json(data_in), dtype=str)


def get_access_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
    }

    payload = {
        "grant_type": GRANT_TYPE,
    }

    try:
        response = requests.post(AUTH_URL, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")








def send_email(subject, body_data, sender, recipients, password):
    # Retrieve the file path of the currently executing script
    script_path = os.path.abspath(__file__)

    # Extract the project directory name from the file path
    project_name = os.path.basename(os.path.dirname(script_path))

    body_template = """
    Greetings Team,

    An error has occurred in {} project. Here are the key details:

    Error Message: {}

    Timestamp: {}

    Best regards,
    IS Team
    """
    body = body_template.format(project_name, *body_data)

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ','.join(recipients)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())

    print("Email sent!")


def send_email_error(error_message):
    # Send error email notification
    subject = "Error Report"
    body_data = [error_message, datetime.utcnow().isoformat() + 'Z']
    sender = EMAIL
    recipients = RECIPIENTS
    password = GOOGLE_PASSWORD

    send_email(subject, body_data, sender, recipients, password)

