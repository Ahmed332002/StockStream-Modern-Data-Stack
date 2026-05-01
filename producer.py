import websocket
import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

value_schema_str = """
{
  "namespace": "com.finnhub",
  "type": "record",
  "name": "StockPrice",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "float"},
    {"name": "timestamp", "type": "long"},
    {"name": "volume", "type": "float"}
  ]
}
"""

API_KEY = 'd7279r1r01qjeeeg6g2gd7279r1r01qjeeeg6g30' 
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092' 

def delivery_report(err, msg):
    if err is not None:
        print(f" Message delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [{msg.partition()}]")


schema_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_client, value_schema_str)

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'value.serializer': avro_serializer
}


producer = SerializingProducer(producer_conf)


def on_message(ws, message):
    data = json.loads(message)
    if data['type'] == 'trade':
        for trade in data['data']:
           
            payload = {
                "symbol": trade['s'],
                "price": float(trade['p']),
                "timestamp": int(trade['t']),
                "volume": float(trade['v'])
            }
            
           
            try:
                producer.produce(topic='finnhub_stocks',key=payload['symbol'], value=payload, on_delivery=delivery_report)
                producer.poll(0) 
                print(f" Sent: {payload['symbol']} @ {payload['price']}")
            except Exception as e:
                print(f" Error producing message: {e}")

def on_error(ws, error):
    print(f" WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(" Connection Closed")

SYMBOLS = [
    'BINANCE:BTCUSDT', 
    'BINANCE:ETHUSDT', 
    'BINANCE:BNBUSDT', 
    'BINANCE:SOLUSDT', 
    'AAPL', 
    'TSLA', 
    'GOOGL',
    'MSFT',
    'AMZN'
]

def on_open(ws):
    print("### Connection Opened ###")
    for symbol in SYMBOLS:
        ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')
        print(f"Subscribed to {symbol}")

if __name__ == "__main__":
  
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever()