"""

Installation:

    pip install --upgrade oauth2client gspread google-api-python-client ZODB zodbpickle twython iso8601 tweepy
"""

import html
import time
import datetime
import json
import BTrees
import ZODB
import ZODB.FileStorage
import os
import sys
from http.client import BadStatusLine

import oauth2client
from oauth2client import tools

import gspread
import transaction

import tweepy

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


JOBS = ["job", "gig", "freelancer", "work", "hire", "hiring", "developer", "contract", "engineer"]


DB_FILE = "gigbot.data.fs"


SPREADSHEET_ID = "15gapk0tmQY5n8kutw3W1TAgfnMSbnEzE_v1MqprKXE0"


LOCATIONS = {
    "San Francisco": "37.781157,-122.398720,25mi",
    "Silicon Valley": "37.4030169,-122.3219799,25mi",
    "Mountain View": "37.403935,-122.1514792,25mi",
    "Berkeley": "37.8719034,-122.2607339,25mi",
}


STACKS = {
    "Python": ["Python", "Django", "Pyramid", "Flask"],  # "Plone"
    "JavaScript": ["JavaScript", "Node", "CoffeeScript"],  # "Angular", "React"
    "DevOp": ["Ansible", "DevOp"],
    "OpSec": ["OpSec", "InfoSec"],
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
        if (location_id in status.text) or (location_id in status.user.location):
            return location_id

    return None


def do_search(db, twitter, stack_id:str, gigword:str, stackword:str, writer:callable, geolocation=None, location_id:str=None):
    """Perform one Twitter search with gig and stack.

    The query can be with Twitter's native geolocation matching or our hacky way.

    Check that the result is in the desired location either from tweet text, tweet place or user location.

    Record Tweets analyzed.
    """

    # Optimize here using since_id
    # Because we are unlikely to hit high volume results we just skip it for now

    if geolocation:
        results = twitter.search(q="{} {}".format(gigword, stackword), geocode=geolocation, lang="en", result_type="recent", count=999)
    else:
        results = twitter.search(q="{} {}".format(gigword, stackword), lang="en", result_type="recent", count=999)

    for status in results:
        tweet_id = status.id_str

        with transaction.manager:
            if tweet_id in db.tweets:
                # We have seen this one earlier
                continue

            # Record it as done

            tweet_link = "https://twitter.com/statuses/{}".format(status.id)
            text = html.unescape(status.text)
            place = str(status.place and status.place.full_name or None)

            # Because agencies etc. don't use geolocation feature of job advertisements, but write it to the tweet body
            # we need to hack a bit here and do manual filtering
            if not geolocation:
                location_id = match_location(status)
                if not location_id:
                    print("Location did not match {} {}".format(tweet_link, status.text))
                    db.tweets[tweet_id] = status
                    continue

            print("Matched @{} {} {} in {}".format(status.user.screen_name, text, status.created_at, location_id))
            if writer(twitter_handle=status.user.screen_name, tweet_link=tweet_link, text=text, created_at=status.created_at, place=place, location=location_id, tweet_id=status.id, stack=stack_id):
                db.tweets[tweet_id] = status
            else:
                # Google API failure
                print("Could not write")

            # Make some love
            try:
                # Causes extra API Traffic... let's try to keep it in minumum
                status.favorite()
                pass
            except tweepy.TweepError:
                # Already favorited
                pass


def attempt_twitter_api(func:callable, throttle_info):

    # Go against the rate limits

    sleep_delay = 60*15

    for attempt_number in range(0, 100):
        try:
            func()
            break
        except tweepy.TweepError as e:
            if e.response.status_code == 429:
                print("Twitter throttling us {}, sleep delay {}, action #{}, attempt #{}".format(e.response.text, sleep_delay, throttle_info, attempt_number))
                # Do so extra sleep when Twitter punishes us
                time.sleep(sleep_delay)
            elif e.response.status_code == 503:
                print("Twitter services overloaded".format(e.response.text, sleep_delay, throttle_info, attempt_number))
                # Do so extra sleep when Twitter punishes us
                time.sleep(sleep_delay)
            else:
                raise e



def do_searches(db, twitter, writer) -> dict:
    """Hit Twitter search with our keyword combos.
    """

    search_num = 0

    # https://dev.twitter.com/rest/public/rate-limiting
    # Search will be rate limited at 180 queries per 15 minute window for the time being, but we may adjust that over time. A friendly reminder that search queries will need to be authenticated in version 1.1.

    for gigword in JOBS:
        for stack, stack_words  in STACKS.items():
            for stackword in stack_words:

                def search_word_match_location():
                    do_search(db, twitter, stack, gigword, stackword, writer, geolocation=None)

                print("Doing word match location search #{}: {} {}".format(search_num, gigword, stackword))
                attempt_twitter_api(search_word_match_location, "Word location match search #{}".format(search_num))
                search_num += 1

                for location_id, location in LOCATIONS.items():

                    def search_geo_match():
                        do_search(db, twitter, stack, gigword, stackword, writer, geolocation=location, location_id=location_id)

                    print("Doing geolocation search #{}: {} {} {}".format(search_num, gigword, stackword, location_id))
                    attempt_twitter_api(search_geo_match, "Geolocation search #{}".format(search_num))
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
        do_searches(db, twitter, writer)
        # Sleep 1/2 hour between bombing
        time.sleep(1800)


def get_spreadsheet(spread, sheet_id) -> tuple:
    """Get write access to spreadsheet and decode column ids."""
    worksheet = spread.open_by_key(sheet_id).sheet1

    # Read first row, decode column ids
    cols = worksheet.row_values(1)

    column_mappings = {col_id: col for col_id, col in enumerate(cols, start=1)}
    return worksheet, column_mappings


# Hack around internal reauthentication issue in gspread
spread = sheet = column_mappings = google_credentials = None

def add_spreadsheet_row(**kwargs) -> bool:
    """A callback which processes the valid Tweet by adding them to a Google spreadsheet."""
    global spread, sheet, column_mappings, google_credentials

    # Map values to the spreadsheet
    cell_mappings = {col_id: kwargs.get(name, "-") for col_id, name in column_mappings.items()}

    # Try few timings as Google services might not be robust
    for attempt in range(0, 3):
        try:
            sheet.append_row(cell_mappings.values())
            return True
        except BadStatusLine as e:
            # https://github.com/burnash/gspread/issues/302
            print("Google API reauthentication failure: {}".format(e))

            # Rebuild Google API client
            google_credentials = get_google_credentials()
            spread = gspread.authorize(google_credentials)
            sheet, column_mappings = get_spreadsheet(spread, SPREADSHEET_ID)

    return False


def main():
    global spread, sheet, column_mappings, google_credentials

    script_name = sys.argv[1] if sys.argv[0] == "python" else sys.argv[0]
    print("Starting {} at {} UTC".format(script_name, datetime.datetime.utcnow()))

    # get OAuth permissions from Google for Drive client and Spreadsheet client
    google_credentials = get_google_credentials()
    spread = gspread.authorize(google_credentials)
    sheet, column_mappings = get_spreadsheet(spread, SPREADSHEET_ID)

    db = get_database()

    print("Found spreadsheet store with mappings of {}".format(column_mappings))
    start_search_loop(db, writer=add_spreadsheet_row)

main()


