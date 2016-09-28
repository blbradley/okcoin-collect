#!/usr/bin/env python

from websocket import create_connection
import json

ws = create_connection('wss://real.okcoin.cn:10440/websocket/okcoinapi')

subs = [
    {'event':'addChannel', 'channel':'ok_sub_spotcny_btc_ticker'},
    {'event':'addChannel', 'channel':'ok_sub_spotcny_btc_depth_60'},
    {'event':'addChannel', 'channel':'ok_sub_spotcny_btc_trades'},
]
msg = json.dumps(subs)

ws.send(msg)

while True:
    response = ws.recv()
    print(response)
