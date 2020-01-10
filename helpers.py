from os import environ
from dotenv import load_dotenv
import math
import pandas as pd
from datetime import datetime, timedelta, date
import requests
import json
from flatten_json import flatten
from jinja2 import Environment, FileSystemLoader
import plaid
from plaid.errors import APIError, ItemError
from plaid import Client
from sqlalchemy import *
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import mapper, sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
import psycopg2
import sqlalchemy as db

def load_tables():
    """"""
    engine = db.create_engine(environ.get('SQLALCHEMY_DATABASE_URI'))
    connection = engine.connect()
    metadata = db.MetaData()
    # automap base
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Transaction = Base.classes.transaction
    session = Session(engine)

    return session, Transaction

def plaidClient():
    client = plaid.Client(environ.get('PLAID_CLIENT_ID'), environ.get('PLAID_SECRET'), environ.get('PLAID_PUBLIC_KEY'), environ.get('PLAID_ENV'), api_version='2018-05-22')
    return client

def getTransactions(client, token, start_date, end_date):
    try:
        account_ids = [account['account_id'] for account in client.Accounts.get(token)['accounts']]
        response = client.Transactions.get(token, start_date, end_date, account_ids=account_ids)
        num_available_transactions = response['total_transactions']
        print("{} Transactions Recieved from Plaid".format(num_available_transactions))
        num_pages = math.ceil(num_available_transactions / 500)
        transactions = []

        for page_num in range(num_pages):
            print("{}% Complete".format(page_num/num_pages * 100))
            transactions += [transaction for transaction in client.Transactions.get(token, start_date, end_date, account_ids=account_ids, offset=page_num * 500, count=500)['transactions']]

        return transactions

    except plaid.errors.PlaidError as e:
        print(json.dumps({'error': {'display_message': e.display_message, 'error_code': e.code, 'error_type': e.type } }))
        transactions = {'result': e.code}

        return transactions

def getData():
    master_data = {}
    today_str = str(date.today())
    client = plaidClient()
    start_date = date.today() - timedelta(days=2)
    trnsx_chase = getTransactions(client, environ.get('ACCESS_TOKEN_Chase'), start_date.strftime('%Y-%m-%d'), today_str)
    trnsx_schwab = getTransactions(client, environ.get('ACCESS_TOKEN_Schwab'), start_date.strftime('%Y-%m-%d'), today_str)
    master_data['all_trnsx'] = trnsx_chase + trnsx_schwab

    return master_data

def frame_prep(transactions, keep_list):
    flat_clean = []
    for t in transactions['all_trnsx']:
        popt = dict([(key, val) for key, val in
               t.items() if key in keep_list])
        flat_clean.append(flatten(popt))

    df = pd.DataFrame(flat_clean)
    df['date'] = pd.to_datetime(df['date'])
    df = df.rename(columns={'category_0': 'category', 'category_1': 'sub_category', 'transaction_id': 't_id', 'pending_transaction_id': 'pending_id'})
    df = df.drop(columns=['category_2'])
    return df

def addTransaction(trnsx_df):
    session, Transaction = load_tables()
    filter_df = trnsx_df[~trnsx_df.t_id.isin([v for v, in session.query(Transaction.t_id).distinct().all()])]
    print('%s Duplicates Found' % (str(len(trnsx_df) - len(filter_df))))
    new_trnsx_d = filter_df.to_dict('records')
    count = 0
    try:
        for t in new_trnsx_d:
            session.add(Transaction(t_id=t['t_id'],
                  name=t['name'],
                  amount=t['amount'],
                  account_id=t['account_id'],
                  date=t['date'],
                  category_id=t['category_id'],
                  category=t['category'],
                  sub_category=t['sub_category'],
                  pending=t['pending'],
                  pending_id=t['pending_id']))
            count += 1
        print('%s New Transactions Added' % (str(count)))
    except Exception as e:
        session.rollback()
        return e
    try:
        session.commit()
        return 'Success'
    except Exception as e:
        session.rollback()
        return e

def tableStyles():
    styles = [dict(selector="caption",
            props=[("text-align", "center"),
                    ("font-size", "200%"),
                    ("color", 'black'),
                    ('font-weight', 'bold')])]
    return styles

def jinjaTEST(data):
    j2_env = Environment(loader=FileSystemLoader('./templates'),
                         trim_blocks=True)
    template_ready = j2_env.get_template('notification.html').render(
            jinja_data=data
        )
    return template_ready
