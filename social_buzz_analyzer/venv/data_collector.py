# data_collector.py

import tweepy
import requests
import pandas as pd
from config import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, CRYPTO_COMPARE_API_KEY
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float

# Setup Twitter API
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

# Setup SQLite
Base = declarative_base()

class CoinData(Base):
    __tablename__ = 'coin_data'
    id = Column(Integer, primary_key=True)
    coin = Column(String)
    mentions = Column(Integer)
    market_cap = Column(Float)
    virality_score = Column(Float)
    hype_to_market_cap = Column(Float)

engine = create_engine('sqlite:///coins.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def discover_new_coins(keyword="crypto"):
    print("Discovering new coins...")
    coin_mentions = {}
    for tweet in tweepy.Cursor(api.search_tweets, q=keyword, lang="en").items(1000):
        for word in tweet.text.split():
            if word.startswith('#'):
                coin = word[1:].lower()
                if coin in coin_mentions:
                    coin_mentions[coin] += 1
                else:
                    coin_mentions[coin] = 1
    sorted_coins = sorted(coin_mentions.items(), key=lambda item: item[1], reverse=True)
    top_coins = [coin for coin, mentions in sorted_coins[:50]]
    print("Discovered coins:", top_coins)
    return top_coins

def get_market_cap(coin_symbol):
    print("Getting market cap data for", coin_symbol, "from CryptoCompare...")
    url = f"https://min-api.cryptocompare.com/data/pricemultifull?fsyms={coin_symbol}&tsyms=USD&api_key={CRYPTO_COMPARE_API_KEY}"
    response = requests.get(url)
    data = response.json()
    if 'RAW' in data and coin_symbol in data['RAW'] and 'USD' in data['RAW'][coin_symbol]:
        return data['RAW'][coin_symbol]['USD']['MKTCAP']
    return None

def get_tweet_engagement(tweet_id):
    print("Getting engagement metrics for tweet with ID", tweet_id, "...")
    tweet = api.get_status(tweet_id)
    likes = tweet.favorite_count
    retweets = tweet.retweet_count
    replies = tweet.reply_count
    shares = 0  
    views = 0   
    virality_score = likes + retweets + replies + shares + views
    print("Virality score for tweet with ID", tweet_id, ":", virality_score)
    return virality_score

def get_twitter_mentions(keyword):
    print("Getting number of mentions for", keyword, "on Twitter...")
    count = 0
    for tweet in tweepy.Cursor(api.search_tweets, q=f"#{keyword}", lang="en").items(100):
        count += 1
    print("Number of mentions for", keyword, ":", count)
    return count

def collect_coin_data(coins):
    print("Collecting data for coins...")
    data = []
    for coin in coins:
        print("Processing coin:", coin)
        mentions = get_twitter_mentions(coin)
        market_cap = get_market_cap(coin.upper())
        if market_cap:
            total_virality_score = 0
            for tweet in tweepy.Cursor(api.search_tweets, q=f"#{coin}", lang="en").items(10):
                virality_score = get_tweet_engagement(tweet.id)
                total_virality_score += virality_score
            virality_score = total_virality_score
            hype_to_market_cap = mentions / market_cap if market_cap > 0 else 0
            data.append({
                'coin': coin,
                'mentions': mentions,
                'market_cap': market_cap,
                'virality_score': virality_score,
                'hype_to_market_cap': hype_to_market_cap
            })
    print("Data collection completed.")
    return pd.DataFrame(data)

def update_data():
    print("Updating data...")
    new_coins = discover_new_coins()
    df = collect_coin_data(new_coins)
    
    session.query(CoinData).delete()
    for _, row in df.iterrows():
        coin_data = CoinData(
            coin=row['coin'],
            mentions=row['mentions'],
            market_cap=row['market_cap'],
            virality_score=row['virality_score'],
            hype_to_market_cap=row['hype_to_market_cap']
        )
        session.add(coin_data)
    session.commit()
    return df
