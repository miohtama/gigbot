""" Authorize server-to-server interactions from Google Compute Engine. """
import os

import oauth2client
import oauth2client.file
from oauth2client import client
from oauth2client import tools

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


# We need permissions to drive list files, drive read files, spreadsheet manipulation
SCOPES = ['https://www.googleapis.com/auth/devstorage.read_write', 'https://www.googleapis.com/auth/drive.metadata.readonly', 'https://spreadsheets.google.com/feeds']
CLIENT_SECRET_FILE = 'google_client_secrets.json'
APPLICATION_NAME = 'Gigbot'
OAUTH_DATABASE = "google_oauth_secrets.json"



def store_google_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """

    # https://developers.google.com/drive/web/quickstart/python

    print("Asking Google Drive and Google Spreadsheet permissions.")
    credential_path = os.path.join(os.getcwd(), OAUTH_DATABASE)

    store = oauth2client.file.Storage(credential_path)
    store.get()

    flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
    flow.user_agent = APPLICATION_NAME
    if flags:
        credentials = tools.run_flow(flow, store, flags)
    else: # Needed only for compatability with Python 2.6
        credentials = tools.run(flow, store)
    print('Storing credentials to ' + credential_path)


if __name__ == "__main__":
    store_google_credentials()