"""

Installation:

    pip install --upgrade oauth2client gspread google-api-python-client ZODB zodbpickle twython iso8601 tweepy
"""
import threading
import html
import time
import datetime
import json
import httplib2
import os
import sys

# Authorize server-to-server interactions from Google Compute Engine.
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

# Date parsing
import iso8601

# https://github.com/burnash/gspread
import gspread

import tweepy


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


#: Debug limiter
MAX_THREADS = 255

#: Throttle connections to Twitter so that they don't get angry
SPIN_DELAY = 1

JOBS = ["job", "gig", "freelancer", "work", "hire", "hiring", "looking for"]

LOCATIONS = {
    "SF": ["San Francisco", "SF", "Bay Area"],
    # "Oakland": ["Oakland"],
    "Silicon Valley": ["Silicon Valley", "Valley"],
    "Mountain View": ["Mountain View"],
    "Berkeley": ["Berkely"],
}

STACKS = {
    "Python": ["Python", "Django", "Pyramid", "Flask"],  # "Plone"
    "JavaScript": ["JavaScript", "JS", "Node"],  # "Angular", "React"
    "CoffeeScript": ["CoffeeScript"],
    "DevOp": ["Ansible", "DevOp"],
}



def get_google_credentials():
    """Gets valid user credentials from storage.

    Returns:
        Credentials, the obtained credential.
    """

    credential_path = os.path.join(os.getcwd(), "google_oauth_secrets.json")
    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    assert credentials, "Please run googleauth.py first"
    return credentials



class GigSearchStreamer(tweepy.StreamListener):

    def __init__(self, api, filter_info:dict, writer):
        super(GigSearchStreamer, self).__init__(api)
        self.filter_info = filter_info
        self.writer = writer

    def on_status(self, status):

        # Twitter makes XSS safe JSON API
        try:
            text = html.unescape(status.text)
            place = status.place and status.place.full_name or None
            tweet_link = "https://twitter.com/statuses/{}".format(status.id)
            self.writer(twitter_handle=status.user.screen_name, tweet_link=tweet_link, text=text, created_at=status.created_at, place=place, tweet_id=status.id, **self.filter_info)

            # Make some love
            status.favorite()
        except Exception as e:
            print(e)  # Ghetto fault handler
            import traceback ; traceback.print_exc()

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        print(exception)


def create_filters() -> dict:
    """Builds Twitter filter() API AND/OR monster.

    https://dev.twitter.com/streaming/overview/request-parameters#track

    :return: dict(track, stack, gigword, location)
    """

    combos = []

    for gigword in JOBS:
        for stack, stack_words  in STACKS.items():
            for stackword in stack_words:
                for location, location_words in LOCATIONS.items():
                    track = "{} {} {}".format(gigword, location, stackword)
                    yield dict(track=track, gigword=gigword, location=location, stack=stack)


def spin_streamer(num, twitter:tweepy.API, filter_info:dict, writer:callable):
    """Start a Python streaming API WebSocket in a new thread."""
    listener = GigSearchStreamer(twitter, filter_info, writer)
    stream = tweepy.Stream(auth = twitter.auth, listener=listener)

    def run():
        stream.filter(track=[filter_info["track"]])

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    print("Spinning up #{} filter for {} in thread {}".format(num, filter_info, thread))



def start_stream_loop(writer:callable):
    """For each search tuple start a new streamer thread."""
    try:
        creds = json.load(open("twitter_oauth_secrets.json", "rt"))
    except IOError:
        raise AssertionError("Please run tweepyauth.py first")

    auth = tweepy.OAuthHandler(creds["consumer_key"], creds["consumer_secret"])
    auth.set_access_token(creds["access_token"], creds["access_token_secret"])
    twitter = tweepy.API(auth)

    for num, filter_info in enumerate(create_filters()):
        spin_streamer(num, twitter, filter_info, writer)
        if num >= MAX_THREADS - 1:
            break
        time.sleep(SPIN_DELAY)

    print("The Sleeping Beauty goes here")
    while True:
        time.sleep(9999999)


def get_spreadsheet(spread, sheet_id) -> tuple:
    """Get write access to spreadsheet and decode column ids."""
    worksheet = spread.open_by_key(sheet_id).sheet1

    # Read first row, decode column ids
    cols = worksheet.row_values(1)

    column_mappings = {col_id: col for col_id, col in enumerate(cols, start=1)}
    return worksheet, column_mappings


def main():

    script_name = sys.argv[1] if sys.argv[0] == "python" else sys.argv[0]
    print("Starting {} at {} UTC".format(script_name, datetime.datetime.utcnow()))

    # get OAuth permissions from Google for Drive client and Spreadsheet client
    credentials = get_google_credentials()
    http = credentials.authorize(httplib2.Http())
    spread = gspread.authorize(credentials)
    sheet, column_mappings = get_spreadsheet(spread, "15gapk0tmQY5n8kutw3W1TAgfnMSbnEzE_v1MqprKXE0")

    print("Found spreadsheet store with mappings of {}".format(column_mappings))
    def add_spreadsheet_row(**kwargs):

        # Map values to the spreadsheet
        cell_mappings = {col_id: kwargs.get(name, "-") for col_id, name in column_mappings.items()}
        sheet.append_row(cell_mappings.values())

    start_stream_loop(writer=add_spreadsheet_row)

main()


