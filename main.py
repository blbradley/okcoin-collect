#!/usr/bin/env python

import os
import logging
import json
from time import sleep
from datetime import datetime

from websocket import create_connection
from requests import Request, Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry


kafka_rest_proxy_url = os.environ['KAFKA_REST_PROXY_URL']
max_retries = int(os.environ['MAX_RETRIES'])

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s')

subs = [
    {'event':'addChannel', 'channel':'ok_sub_spotcny_btc_ticker'},
    {'event':'addChannel', 'channel':'ok_sub_spotcny_btc_depth_60'},
    {'event':'addChannel', 'channel':'ok_sub_spotcny_btc_trades'},
]
initial_msg = json.dumps(subs)

key_schema = json.dumps({'type': 'string'})
value_schema = json.dumps({
    'type': 'record',
    'name': 'ExchangeEvent',
    'fields': [
        {'name': 'time', 'type': 'string'},
        {'name': 'exchange', 'type': 'string'},
        {'name': 'event', 'type': 'string'},
    ]
})

s = Session()

def get_broker_ids():
    req = s.get(kafka_rest_proxy_url + '/brokers').json()
    return req['brokers']

def create_producer_request(key, value):
    headers = {'Content-Type':'application/vnd.kafka.avro.v1+json'}
    data = {'key_schema': key_schema, 'value_schema': value_schema, 'records': [{'key': key, 'value': value}]}
    return Request('POST', kafka_rest_proxy_url + '/topics/exchanges_raw', data=json.dumps(data), headers=headers)

def main():
    if not get_broker_ids():
        raise RuntimeError('No brokers connected.')

    ws = create_connection('wss://real.okcoin.cn:10440/websocket/okcoinapi')
    ws.send(initial_msg)

    while True:
        response = ws.recv()

        timestamp = datetime.utcnow()
        exchange = 'okcoin'
        value = {'time': timestamp.isoformat(), 'exchange': exchange, 'event': response}
        req = create_producer_request(exchange, value)

        prepped = s.prepare_request(req)
        retry_count = 0
        while retry_count < max_retries:
            resp = s.send(prepped)
            if resp.status_code == 200:
                break
            logging.error('[%s] %d %s', resp.headers['Date'], resp.status_code, resp.text)

            # fixed retry delay
            sleep(3)
            retry_count += 1
        if retry_count >= max_retries:
            raise RuntimeError('Exceeded maximum number of retries.')

if __name__ == "__main__":
    main()
