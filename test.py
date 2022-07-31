from plistlib import load
import slack
import os
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask
from slackeventsapi import SlackEventAdapter
import mysql.connector as sql
import pandas as pd
import matplotlib.pyplot as plt
import random




env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(
    os.environ['SIGNING_SECRET'], '/slack/events', app)

client = slack.WebClient()

client = slack.WebClient(token=os.environ['SLACK_TOKEN_'])
BOT_ID = client.api_call("auth.test")['user_id']
# client.chat_postMessage(channel='#text2sql', text='Hello Raghu')

c=sql.connect(host='localhost',
    port=3306,
    database='sakila',
    user='root',
    password ='9758'
)



actor_df = pd.read_sql('select * from actor', c)

def famous_actor():
    a = ('select concat(first_name," ", last_name), ' +
         'count(*) '+
         'from actor ' +
         'group by first_name, last_name')

    return pd.read_sql(a,c)


def image_return():
    dtf = pd.read_sql(' SELECT name AS category, count(*) AS count_ \
        FROM film AS F \
        JOIN film_category AS FC USING(film_id) \
        JOIN category AS C USING(category_id) \
        GROUP BY category_id', c)
    dtf = dtf.set_index('category')

    name = str(random.getrandbits(128)) + ".png"
    plot = dtf.plot(kind='pie', y='count_', legend=False)
    fig = plot.get_figure()
    fig.savefig("images/"+name)
    return name

def csv_return():
    dtf = pd.read_sql(' SELECT name AS category, count(*) AS count_ \
        FROM film AS F \
        JOIN film_category AS FC USING(film_id) \
        JOIN category AS C USING(category_id) \
        GROUP BY category_id', c)
    dtf = dtf.set_index('category')

    name = str(random.getrandbits(128)) + ".csv"
    dtf.to_csv('data/'+name)
    
    return name

def double_or_1(target):

    count_steps = 0

    while target != 1:
        if target % 2 == 0:
            target /= 2
        else:
            target -= 1
        # print(target, count_steps)
        count_steps += 1

    return count_steps

@slack_event_adapter.on('message')
def message(payload):
    event = payload.get('event', {})
    channel_id = event.get('channel')
    user_id = event.get('user')
    text2 = event.get('text')
    # response = int(text2.split(" ")[1])
    
    
    if BOT_ID != user_id and text2 == "query":
        # client.chat_postMessage(
        #     channel='#text2sql',
        #     text='Hello')
        
        # img_name = str(image_return())
        # img = open("images/"+img_name, 'rb').read()
        # client.files_upload(
        #     channels = "#text2sql",
        #     initial_comment = "That's one small step for man, one giant leap for mankind.",
        #     filename = "text2",
        #     content = img)
        
        csv_name = str(csv_return())
        csv = open("data/"+csv_name, 'rb').read()
        client.files_upload(
            channels = "#text2sql",
            initial_comment = "That's one small step for man, one giant leap for mankind.",
            filename = "text2",
            content = csv)


if __name__ == "__main__":
    app.run(debug=True)