"""

Installation:

    pip install --upgrade oauth2client gspread google-api-python-client ZODB zodbpickle twython iso8601 tweepy
"""
import threading
import html
import time
import datetime
import json
import BTrees
import ZODB
import ZODB.FileStorage
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
import transaction

import tweepy


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


#: Debug limiter
MAX_THREADS = 150

#: Throttle connections to Twitter so that they don't get angry
SPIN_DELAY = 15

JOBS = ["job", "gig", "freelancer", "work", "hire", "hiring", "looking for", "developer"]

DB_FILE = "gigbot.data.fs"

LOCATIONS = {
    "SF": ["San Francisco", "SF", "Bay Area"],
    # "Oakland": ["Oakland"],
    "Silicon Valley": ["Silicon Valley"],
    "Mountain View": ["Mountain View", "#MountainView"],
    "Berkeley": ["Berkely"],
    "London": ["London"]
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


def get_database():
    """Get or create a ZODB database where we store information about processed spreadsheets and sent tweets."""

    storage = ZODB.FileStorage.FileStorage(DB_FILE)
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root

    # Initialize root data structure if not present yet
    with transaction.manager:
        if not hasattr(root, "tweets"):
            root.tweets = BTrees.OOBTree.BTree()


    return root



# class GigSearchStreamer(tweepy.StreamListener):
#     """XXX: Twitter has a limit of max 3 streaming connections per user so this did not work out."""
#
#     def __init__(self, api, filter_info:dict, writer):
#         super(GigSearchStreamer, self).__init__(api)
#         self.filter_info = filter_info
#         self.writer = writer
#
#     def on_status(self, status):
#
#         # Twitter makes XSS safe JSON API
#         try:
#             tweet_link = "https://twitter.com/statuses/{}".format(status.id)
#             text = html.unescape(status.text)
#
#             # Match location inside the tweet itself or by the user location
#             for location_id, location_texts in LOCATIONS.items():
#                 for location_text in location_texts:
#                     if location_text in text or location_text in status.user.location:
#                         break
#             else:
#                 # Location was not matched in tweet or user place
#                 print("Location did not match {} {}".format(tweet_link, text))
#                 return
#
#             place = status.place and status.place.full_name or None
#
#             print("Matched {} {}".format(tweet_link, text))
#             self.writer(twitter_handle=status.user.screen_name, tweet_link=tweet_link, text=text, created_at=status.created_at, place=place, matched_location=location_id, tweet_id=status.id, **self.filter_info)
#
#             # Make some love
#             status.favorite()
#         except Exception as e:
#             print(e)  # Ghetto fault handler
#             import traceback ; traceback.print_exc()
#
#     def on_exception(self, exception):
#         """Called when an unhandled exception occurs."""
#         print(exception)
#
#     def on_disconnect(self, notice):
#         """Called when twitter sends a disconnect notice
#
#         Disconnect codes are listed here:
#         https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect
#         """
#         print("Disconnect", notice)
#
#     def on_warning(self, notice):
#         """Called when a disconnection warning message arrives"""
#         print("Warning", notice)
#
#     def on_error(self, status_code):
#         """Called when a non-200 status code is returned"""
#
#         # We need to implement error handling, or otherwise HTTP 420 throttling does not get kick in and we don't know it is happening
#         print("Twitter API gave us error: {}".format(status_code))
#         return True


def match_location(status):
    """Match location inside the tweet itself or by the user location to our criteria."""
    for location_id, location_texts in LOCATIONS.items():
        for location_text in location_texts:
            if (location_text in text) or (location_text in status.user.location):
                return location_id

    return None

def do_search(db, twitter, gigword, stackword):
    """Perform one Twitter search with gig and stack.

    Check that the result is in the desired location either from tweet text, tweet place or user location.

    Record Tweets analyzed.
    """

    # Optimize here using since_id
    # Because we are unlikely to hit high volume results we just skip it for now
    results = twitter.search(q="{} {}".format(gigword, stackword), lang="en", result_type="recent")
    for status in results:
        tweet_id = status.id_str

        with transaction.manager:
            if tweet_id in db.tweets:
                # We have seen this one earlier
                continue

            db.tweets[tweet_id] = status

            tweet_link = "https://twitter.com/statuses/{}".format(status.id)
            text = html.unescape(status.text)

            location_id = match_location(status)
            if not location_id:
                # Location was not matched in tweet or user place
                print("Location did not match {} {}".format(tweet_link, text))
                return

            print("Matched {} {}".format(tweet_link, text))
            self.writer(twitter_handle=status.user.screen_name, tweet_link=tweet_link, text=text, created_at=status.created_at, place=place, matched_location=location_id, tweet_id=status.id)

            # Make some love
            status.favorite()


def do_searches(db, twitter) -> dict:
    """Builds Twitter filter() API AND/OR monster.

    https://dev.twitter.com/streaming/overview/request-parameters#track

    :return: dict(track, stack, gigword, location)
    """

    combos = []

    sleep_delay = 3
    search_num = 0

    for gigword in JOBS:
        for stack, stack_words  in STACKS.items():
            for stackword in stack_words:
                for attempt_number in range(0, 100):
                    try:
                        time.sleep(sleep_delay)
                        do_search(db, twitter, gigword, stackword)
                        sleep_delay *= 0.99
                        break
                    except tweepy.TweepError as e:
                        if e.response.status_code == 429:
                            print("Twitter throttling us {}, sleep delay {}, search #{}, attempt #{}".format(e.response.text, sleep_delay, search_num, attempt_number))
                            # Do so extra sleep when Twitter punishes us
                            time.sleep(sleep_delay)
                            sleep_delay *= 2
                search_num += 1




def start_search_loop(db, writer:callable):
    """For each search tuple start a new streamer thread."""
    try:
        creds = json.load(open("twitter_oauth_secrets.json", "rt"))
    except IOError:
        raise AssertionError("Please run tweepyauth.py first")

    auth = tweepy.OAuthHandler(creds["consumer_key"], creds["consumer_secret"])
    auth.set_access_token(creds["access_token"], creds["access_token_secret"])
    twitter = tweepy.API(auth)

    while True:
        do_searches(db, twitter)
        # Sleep 1/2 hour between bombing
        time.sleep(1800)


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

    db = get_database()

    print("Found spreadsheet store with mappings of {}".format(column_mappings))
    def add_spreadsheet_row(**kwargs):

        # Map values to the spreadsheet
        cell_mappings = {col_id: kwargs.get(name, "-") for col_id, name in column_mappings.items()}
        sheet.append_row(cell_mappings.values())

    start_search_loop(db, writer=add_spreadsheet_row)

main()


