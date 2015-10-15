"""Authorize Twitter API access."""
import json
import webbrowser

import tweepy


if __name__ == "__main__":

    consumer_key = input('Consumer key: ').strip()
    consumer_secret = input('Consumer secret: ').strip()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    # Open authorization URL in browser
    webbrowser.open(auth.get_authorization_url())

    # Ask user for verifier pin
    pin = input('Verification pin number from twitter.com: ').strip()

    # Get access token
    access_token, access_token_secret = auth.get_access_token(verifier=pin)

    data = dict(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token, access_token_secret=access_token_secret)
    with open("twitter_oauth_secrets.json", "wt") as f:
        json.dump(data, f)

