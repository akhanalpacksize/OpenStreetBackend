DEBUG = False

BASE_URL = 'https://api.domo.com/v1'

GRANT_TYPE = 'client_credentials'


AUTH_URL = "https://api.domo.com/oauth/token"

# **** ENV FOR PRODUCTION PURPOSE ****
CLIENT_ID = 'e3bcf33e-cad7-4668-b65e-fc01630a8d47'
CLIENT_SECRET = '20fa36206a52cd940ccfadabc1211aaf1662d0ba6049511f403ef14deddbebd9'

# GRANT TYPE
GRANT_TYPE = 'client_credentials'

API_KEY_OSM = '5b3ce3597851110001cf6248c9367aa0f1b042b99b70644e32da3ab7'

ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"

SALESFORCE_ID = 'cf7aaac4-f6cf-4415-8c42-7ed458cf1599'

Road_distance_ID = 'ae3bf3a4-ef26-4b4c-a9f5-d5da819f3167'




# GOOGLE APP PASSWORD
GOOGLE_PASSWORD = 'esxknqizywqtfoph'

# EMAIL OF THE SENDER
EMAIL = 'ayush.khanal@packsize.com'

# EMAIL TO BE SENT TO
# RECIPIENTS = ['ayush.khanal@packsize.com', 'uddhav.dahal@packsize.com']
RECIPIENTS = ['ayush.khanal@packsize.com']
instance = 'Prod'


if DEBUG:
    # **** ENV FOR LOCAL OR DEVELOPMENT PURPOSE ****
    # CLIENT ID
    CLIENT_ID = '072dd439-1975-4113-98d9-6dcbbf231603'
    CLIENT_SECRET = '60121d3e9fc90b8bdb9ef189b16b654fd4bcd7815ce41b4ea40eba0565f729aa'
    # GOOGLE APP PASSWORD
    GOOGLE_PASSWORD = 'esxknqizywqtfoph'

    # EMAIL OF THE SENDER
    EMAIL = 'ayush.khanal@packsize.com'

    # EMAIL TO BE SENT TO
    RECIPIENTS = ['ayush.khanal@packsize.com']

    instance = 'local'





print("DEBUG: ", DEBUG)
print("RECIPIENTS: ", RECIPIENTS)
print('instance', instance)