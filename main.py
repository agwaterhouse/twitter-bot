import tweepy
from urllib3.exceptions import ProtocolError
from urllib3 import exceptions as ec
import time
from dotenv import load_dotenv
import csv
import os
import gspread
from gspread.models import Cell
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

API_KEY = os.environ.get('API_KEY')
API_SECRET_KEY = os.environ.get('API_SECRET_KEY')
ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET')


def fetch():
        scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)
        Settings = client.open("Keywords").sheet1

        Keywords = []
        for row in Settings.get_all_values():
            for value in row:
                if value != '':
                    Keywords.append(value.lower())
        
        return Keywords

global keywords
keywords = fetch()

def update(row):
        scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)
        words = client.open("Data").sheet1

        words.append_row(row)
        print('Updated row')

class MaxListener(tweepy.StreamListener):

    def __init__(self, api):
        self.api = api

    def on_status(self, status):

    
        if not status.truncated:
            tweet_text = status.text
        else:
            tweet_text = status.extended_tweet['full_text']

        
        for word in tweet_text.split(' '):
            if word.lower() in keywords:
                row = [str(status.created_at), tweet_text, f'https://twitter.com/twitter/statuses/{status.id}']
                update(row)
                break
            else:
                pass

    def on_error(self, error):
        print(error)
        return False


class MaxStream():
    
    def __init__(self, api, listener):
        self.stream = tweepy.Stream(auth=api.auth, listener = listener)

    def start(self):
        
        keywords = self.fetch()
        print(keywords)
        print('Starting Stream')
        while True:
            try:
                self.stream.filter(track = keywords)
            except ProtocolError as pe:
                print(pe)
                continue
            except ec.TimeoutError as te:
                print(te)
                continue
            except ec.HTTPError as  he:
                print(he)
                continue
    
    def fetch(self):
        scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)
        Settings = client.open("Settings").sheet1

        users = []
        for row in Settings.get_all_values():
            for value in row:
                if value != '':
                    users.append('@' + value)
        
        return users

if __name__ == "__main__":
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)
    listener = MaxListener(api)
    stream  = MaxStream(api, listener)
    stream.start()