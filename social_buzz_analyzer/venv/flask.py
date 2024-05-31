from flask import Flask, render_template
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_collector import update_data, CoinData

app = Flask(__name__)

# Setup SQLite
engine = create_engine('sqlite:///coins.db')
Session = sessionmaker(bind=engine)
session = Session()

def get_latest_data():
    query = session.query(CoinData).all()
    data = []
    for coin_data in query:
        data.append({
            'coin': coin_data.coin,
            'mentions': coin_data.mentions,
            'market_cap': coin_data.market_cap,
            'virality_score': coin_data.virality_score,
            'hype_to_market_cap': coin_data.hype_to_market_cap
        })
    return pd.DataFrame(data)

@app.route('/')
def index():
    df = get_latest_data()
    df_sorted = df.sort_values(by='hype_to_market_cap', ascending=False)

    fig = px.bar(df_sorted, x='coin', y='hype_to_market_cap', title='Hype to Market Cap Ratio of Cryptocurrencies')
    graph_html = fig.to_html(full_html=False)

    return render_template('index.html', graph_html=graph_html, table=df_sorted.to_html(classes='table table-striped'))

if __name__ == '__main__':
    update_data()
    app.run(debug=True)
