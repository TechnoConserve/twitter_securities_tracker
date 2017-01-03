from contextlib import contextmanager
from datetime import datetime
from pytz import timezone
import thread
from threading import Timer
from time import sleep

from ib.ext.Contract import Contract
import numpy as np
import schedule
from requests.exceptions import ChunkedEncodingError, ConnectionError
from sqlalchemy import Column, DateTime, Float, Integer, String, Text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from twython import Twython, TwythonError, TwythonStreamer

from ib_interface import PriceGrabber
from local_secret import APP_KEY
from local_secret import APP_SECRET
from local_secret import OAUTH_TOKEN
from local_secret import OAUTH_TOKEN_SECRET
from local_secret import sentiment_engine, securities_engine

Base = declarative_base()
sentiment_session = sessionmaker(bind=sentiment_engine)
securities_session = sessionmaker(bind=securities_engine)

IBUtil = PriceGrabber()

local_tz = timezone('US/Mountain')
eastern = timezone('US/Eastern')


@contextmanager
def session_scope(db='sentiment'):
    """Provide a transactional scope around a series of operations."""
    # Determine which database to connect to
    if db == 'sentiment':
        session = sentiment_session()
    else:
        session = securities_session()

    retry = 2
    while retry:
        retry -= 1
        try:
            yield session
            session.commit()
        except DBAPIError as exc:
            if retry and exc.connection_invalidated:
                session.rollback()
            else:
                raise
        finally:
            session.close()
            retry = 0


class Tweet(Base):
    __tablename__ = 'tweets'

    id = Column(Integer, primary_key=True)
    user = Column(String(15))           # Twitter handle
    symbol = Column(String(5))          # Ticker symbol
    words = Column(Text)                # Content of tweet as a string
    time = Column(DateTime)             # Time of tweet posting
    price = Column(Float(4))            # Price of security at time of posting
    hour_1 = Column(Float(4), nullable=True)           # Price of security 1 hour after posting
    hour_1_per_chng = Column(Float(2), nullable=True)  # Percent change after 1 hour relative to original price
    day_1 = Column(Float(4), nullable=True)            # Price of security 1 day after posting
    day_1_per_chng = Column(Float(2), nullable=True)   # Percent change after 1 day relative to original price
    day_3 = Column(Float(4), nullable=True)            # Price of security 3 days after posting
    day_3_per_chng = Column(Float(2), nullable=True)   # Percent change after 3 days relative to original price
    day_7 = Column(Float(4), nullable=True)            # Price of security 7 days after posting
    day_7_per_chng = Column(Float(2), nullable=True)   # Percent change after 7 days relative to original price
    day_30 = Column(Float(4), nullable=True)           # Price of security 30 days after posting
    day_30_per_chng = Column(Float(2), nullable=True)  # Percent change after 30 days relative to original price

    def __repr__(self):
        return "<Tweet (@'%s'):\n['%s']\n" \
               "[Original Price]\t'%s'\n" \
               "[Hour 1 Price]\t'%s'\n" \
               "[Hour 1 % Change]\t'%s'\n" \
               "[Day 1 Price]\t'%s'\n" \
               "[Day 1 % Change]\t'%s'\n" \
               "[Day 3 Price]\t'%s'\n" \
               "[Day 3 % Change]\t'%s'\n" \
               "[Day 7 Price]\t'%s'\n" \
               "[Day 7 % Change]\t'%s'\n" \
               "[Day 30 Price]\t'%s'\n" \
               "[Day 30 % Change]\t'%s'>" % (
            self.user, self.words, self.price, self.hour_1, self.hour_1_per_chng,
            self.day_1, self.day_1_per_chng, self.day_3, self.day_3_per_chng,
            self.day_7, self.day_7_per_chng, self.day_30, self.day_30_per_chng
        )


class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        # Only interested in tweets during (and just before) market hours
        if check_time():
            if 'text' in data and data['entities']['symbols']:
                payload = data['text'].encode('utf-8')
                handle = check_user(data['user']['id'])
                timestamp = datetime.now()
                print '\n[!] New Tweet by @{}:'.format(handle)
                print(payload)
                for ticker in data['entities']['symbols']:
                    ticker = ticker['text'].encode('utf-8')
                    print "Found Ticker!: ", ticker
                    price = get_current_price(ticker)
                    if price is None:
                        print('No price found. Skipping...')
                        break
                    print "Current Price: ", price
                    tweet_id = add_to_database(handle, ticker, payload, timestamp, price)
                    start_timers(tweet_id)

    def on_error(self, status_code, data):
        print 'Error code: {}'.format(status_code)


def initialize():
    Base.metadata.create_all(sentiment_engine)


def add_to_database(handle, ticker, payload, timestamp, price):
    with session_scope() as session:
        new_tweet = Tweet(user=handle, symbol=ticker, words=payload, time=timestamp,
                          price=price)
        session.add(new_tweet)
        session.commit()
        return new_tweet.id


def check_time():
    """
    Is it a weekday between 9 AM and 4 PM EST?
    """
    current_time_local = local_tz.localize(datetime.now())
    current_time_est = current_time_local.astimezone(eastern)
    if current_time_est.isoweekday() in range(1, 6) \
            and current_time_est.hour in range(9, 16):
        return True


def check_user(user_id):
    twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    return twitter.show_user(user_id=user_id)['screen_name']


def daily_check():
    today = datetime.today()
    with session_scope() as session:
        for tweet_id, past_time in session.query(Tweet.id, Tweet.time):
            days = np.busday_count(past_time, today)
            print("Checking if price needs updating for tweet number {} which was tweeted {} days ago."
                  .format(tweet_id, days))
            if days == 1:
                # One day has passed since the tweet
                update_database(tweet_id, 'day1')
            elif days == 3:
                # Three days have passed since the tweet
                update_database(tweet_id, 'day3')
            elif days == 7:
                # Seven days have passed since the tweet
                update_database(tweet_id, 'day7')
            elif days == 30:
                # 30 days have passed since the tweet
                update_database(tweet_id, 'day30')
            else:
                print("Nah, it's all good.")


def get_current_price(ticker):
    c = Contract()
    c.m_symbol = ticker
    c.m_secType = 'STK'
    c.m_exchange = 'SMART'
    c.m_currency = 'USD'
    IBUtil.request_data(c)
    sleep(3)
    price = IBUtil.price.pop(ticker, None)
    return price


def start_timers(tweet_id):
    hourly = Timer(3600.0, update_database, [tweet_id, 'hourly'])
    hourly.start()


def update_database(tweet_id, period):
    with session_scope() as session:
        tweet = session.query(Tweet).get(tweet_id)
        current_price = get_current_price(tweet.symbol)
        if current_price is None:
            print('Price data not currently available. Skipping database update for tweet {} ({})'
                  .format(tweet_id, tweet.symbol))
            return
        elif period == 'hourly':
            print('Updating database with hour 1 price info for tweet {} ({})'.format(tweet_id, tweet.symbol))
            tweet.hour_1 = current_price
            tweet.hour_1_per_chng = ((tweet.price - tweet.hour_1) / tweet.hour_1) * 100
        elif period == 'day1':
            print('Updating database with day 1 price info for tweet {} ({})'.format(tweet_id, tweet.symbol))
            tweet.day_1 = current_price
            tweet.day_1_per_chng = ((tweet.price - tweet.day_1) / tweet.day_1) * 100
        elif period == 'day3':
            print('Updating database with day 3 price info for tweet {} ({})'.format(tweet_id, tweet.symbol))
            tweet.day_3 = current_price
            tweet.day_3_per_chng = ((tweet.price - tweet.day_3) / tweet.day_3) * 100
        elif period == 'day7':
            print('Updating database with day 7 price info for tweet {} ({})'.format(tweet_id, tweet.symbol))
            tweet.day_7 = current_price
            tweet.day_7_per_chng = ((tweet.price - tweet.day_7) / tweet.day_7) * 100
        elif period == 'day30':
            print('Updating database with day 30 price info for tweet {} ({})'.format(tweet_id, tweet.symbol))
            tweet.day_30 = current_price
            tweet.day_30_per_chng = ((tweet.price - tweet.day_30) / tweet.day_30) * 100


def main():
    initialize()
    # Then we run it again every day shortly after market close
    schedule.every().monday.at('14:30').do(daily_check)
    schedule.every().tuesday.at('14:30').do(daily_check)
    schedule.every().wednesday.at('14:30').do(daily_check)
    schedule.every().thursday.at('14:30').do(daily_check)
    schedule.every().friday.at('14:30').do(daily_check)
    schedule.run_continuously(30)
    stream = MyStreamer(APP_KEY, APP_SECRET,
                        OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    while True:
        try:
            # Gets stream from authenticated user: @TechnoConserve
            thread.start_new_thread(stream.user())
        # Can happen when internet cuts out unexpectedly
        except ChunkedEncodingError:
            sleep(60)
        except ConnectionError:
            sleep(60)
        # So program doesn't hang when I manually interrupt it
        except KeyboardInterrupt:
            break
        except TwythonError:
            print('[!] Error: ', TwythonError.msg)


if __name__ == '__main__':
    main()
