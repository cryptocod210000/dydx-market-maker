#Dependencies
import websocket, json, time
from sortedcontainers import SortedDict
from decimal import Decimal
from web3 import Web3
import pandas as pd

#dYdX dependencies
from dydx3 import Client
from dydx3.constants import *
from dydx3.constants import POSITION_STATUS_OPEN
from decimal import Decimal

ETHEREUM_ADDRESS = '#INSERT YOU ETHEREUM ADDRESS HERE'
private_client = Client(
    host='https://api.dydx.exchange',
    api_key_credentials={ 'key': '#INSERT YOUR API KEY HERE', 
                         'secret': '#INSERT YOUR API SECRET HERE', 
                         'passphrase': '#INSERT YOUR API PASSPHRASE HERE'},
    stark_private_key='#INSERT YOUR STARK PRIVATE KEY HERE',
    default_ethereum_address=ETHEREUM_ADDRESS,
)

account_response = private_client.private.get_account()
position_id = account_response.data['account']['positionId']

security_name =  "AVAX-USD" ##Change Market Pair Here
size = 1 ##Change Market size here
pct_spread = 0.1 ##Change spread charged here

dicts = {}
dicts['bids'] = {}
dicts['asks'] = {}

offsets = {}

count = 1
skew = "buy"
bid_order_id = 0
ask_order_id = 0
position_balance_id = 0 #order id of postion clearing trade

def parse_message(msg_):
    global dicts, offsets 
    
    if msg_["type"] == "subscribed":
        for side, data in msg_['contents'].items():
            for entry in data:
                    size = Decimal(entry['size'])
                    if size > 0:
                        price = Decimal(entry['price'])
                        dicts[str(side)][price] = size

                        offset = Decimal(entry["offset"])
                        offsets[price] = offset
    
    if msg_["type"] == "channel_data":
        #parse updates
        for side, data in msg_['contents'].items():
            if side == 'offset':
                offset = int(data)
                continue
            else:
            	for entry in data:
                	price = Decimal(entry[0])
                	amount = Decimal(entry[1])

                	if price in offsets and offset <= offsets[price]:
                		continue
                
                	offsets[price] = offset
                	if amount == 0:
                		if price in dicts[side]:
                			del dicts[side][price]
                	else:
                		try:
                			dicts[side].append((price, amount))
                		except AttributeError:
                			dicts[side][price] = amount

def run_script():
	def on_open(ws):
	    print("opened")
	    channel_data = { "type": "subscribe", "channel": "v3_orderbook", "id": str(security_name), "includeOffsets": "True"}
	    ws.send(json.dumps(channel_data))

	def on_message(ws, message):
	    global dicts, count, skew, bid_order_id, ask_order_id, position_balance_id, position_id
	    
	    obj = json.loads(message)
	    parse_message(obj)
	    
	    best_bid = max(dicts["bids"].keys())
	    best_ask = min(dicts["asks"].keys())
	    
	    print(str(count) + " " , " bid " + str(best_bid) + " / " + str(dicts["bids"][Decimal(best_bid)]), " - ask " + str(best_ask) + " / " + str(dicts["asks"][Decimal(best_ask)]))

	    if best_bid >= best_ask: #check for any inverse orderbook
	    	print("INVERSE")
	    	ws.close()
	    	print("closed")
	    	dicts = {}
	    	dicts['bids'] = {}
	    	dicts['asks'] = {}
	    	offsets = {}
	    	count +=1
	    	private_client.private.cancel_all_orders(market=security_name)
	    	time.sleep(5) 
	    	run_script()

	    if count%149 == 0:
	            
	        try:
	            private_client.private.cancel_order(order_id=str(bid_order_id))
	        except Exception as e:
	        	count +=1
	        	print(e)

	        try:
	            private_client.private.cancel_order(order_id=str(ask_order_id))
	        except Exception as e:
	        	count +=1
	        	print(e)

	        bid_order_price = best_bid - (best_bid * Decimal(pct_spread)/100)
	        ask_order_price = best_ask + (best_ask * Decimal(pct_spread)/100)

	        try:
        		order_params = {'position_id': position_id, 'market': security_name, 'side': ORDER_SIDE_BUY,
        		'order_type': ORDER_TYPE_LIMIT, 'post_only': True, 'size': str(size), 
        		'price': str(round(bid_order_price,1)), 'limit_fee': '0.0015',
        		'expiration_epoch_seconds': time.time() + 120}
        		count +=1
        		bid_order_dict = private_client.private.create_order(**order_params)
        		bid_order_id = bid_order_dict.data['order']['id']
        		print("bid submitted at " + bid_order_dict.data['order']['price'])
        	except Exception as e:
        		print(e)

        	try:
        		order_params = {'position_id': position_id, 'market': security_name, 'side': ORDER_SIDE_SELL,
        		'order_type': ORDER_TYPE_LIMIT, 'post_only': True, 'size': str(size),
        		'price': str(round(ask_order_price,1)), 'limit_fee': '0.0015',
        		'expiration_epoch_seconds': time.time() + 120}
        		count +=1
        		ask_order_dict = private_client.private.create_order(**order_params)
        		ask_order_id = ask_order_dict.data['order']['id']
        		print("ask submitted at " + ask_order_dict.data['order']['price'])
        	except Exception as e:
        		print(e)

	    elif count %419 == 0:

	    	try:
	    		private_client.private.cancel_order(order_id=str(position_balance_id))
	    	except Exception as e:
	    		count +=1
	    		print(e)

	    	all_positions = private_client.private.get_positions(
	    		market=security_name,
	    		status=POSITION_STATUS_OPEN,
	    		)

	    	try:
		    	if (all_positions.data["positions"][0]["side"] == "LONG") and (abs(int(all_positions.data["positions"][0]["size"])) != 0):
		    		position_entry_clear = max(float(all_positions.data["positions"][0]["entryPrice"]) * (1 + pct_spread/100), best_ask)
		    		position_size_clear = abs(int(all_positions.data["positions"][0]["size"]))
		    		order_params = {
		    			'position_id': position_id,
		    			'market': security_name,
		    			'side': ORDER_SIDE_SELL,
		    			'order_type': ORDER_TYPE_LIMIT,
		    			'post_only': True,
		    			'size': str(position_size_clear) ,
		    			'price': str(round(position_entry_clear,rounding_decimal)) ,
		    			'limit_fee': '0.0015',
		    			'expiration_epoch_seconds': time.time() + 120,
		    			}

		    		try:
		    			position_clear_sell_order_dict = private_client.private.create_order(**order_params)
		    			position_balance_id = position_clear_sell_order_dict.data["order"]['id']
		    			print("clearance sell submitted at " + position_clear_sell_order_dict.data['order']['price'])
		    		except Exception as e:
		    			print(e)

		    		count +=1

		    	elif (all_positions.data["positions"][0]["side"] == "SHORT") and (abs(int(all_positions.data["positions"][0]["size"])) != 0):
		    		position_entry_clear = min(float(all_positions.data["positions"][0]["entryPrice"]) * (1 - pct_spread/100), best_bid)
		    		position_size_clear = abs(int(all_positions.data["positions"][0]["size"]))

		    		order_params = {
		    			'position_id': position_id,
		    			'market': security_name,
		    			'side': ORDER_SIDE_BUY,
		    			'order_type': ORDER_TYPE_LIMIT,
		    			'post_only': True,
		    			'size': str(position_size_clear) ,
		    			'price': str(round(position_entry_clear,rounding_decimal)) ,
		    			'limit_fee': '0.0015',
		    			'expiration_epoch_seconds': time.time() + 120,
		    			}
		    		try:
		    			position_clear_buy_order_dict = private_client.private.create_order(**order_params)
		    			position_balance_id = position_clear_buy_order_dict.data["order"]['id']
		    			print("clearance buy submitted at " + position_clear_buy_order_dict.data['order']['price'])
		    		except Exception as e:
		    			print(e)
		    		count +=1

	    	except IndexError:
		    	print("No positions")
		    	count +=1

	    elif count %2437 == 0:
	    	private_client.private.cancel_all_orders(market=security_name)
	    	s.close()
	    	print("closed")
	    	dicts = {}
	    	dicts['bids'] = {}
	    	dicts['asks'] = {}
	    	offsets = {}
	    	count = 0 
	    	run_script()
	    	
	    else:
	    	count = count + 1
		
	def on_close(ws):
	    print("### closed ###")
	    
	socket = "wss://api.dydx.exchange/v3/ws"
	ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
	ws.run_forever()


if __name__ == "__main__":
    try:
        run_script()
    except Exception as err:
        print(err)
        print("connect failed")