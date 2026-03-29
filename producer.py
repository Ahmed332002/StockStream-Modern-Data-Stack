import websocket
import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# 1. تعريف السكيما (Avro Schema) - ده "العقد" اللي بيضمن جودة البيانات
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

# 2. إعدادات الاتصال (تأكد من الـ API Key الخاص بك)
API_KEY = 'd7279r1r01qjeeeg6g2gd7279r1r01qjeeeg6g30' 
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092' # بورت 29092 عشان إحنا "بره" الدوكر

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# 3. إعداد الـ Schema Registry Client والـ Serializer
schema_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_client, value_schema_str)

# 4. إعداد الـ Producer
# شيلنا 'dr_cb' من الـ Dictionary وحطيناها في الـ Constructor تحت
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'value.serializer': avro_serializer
}

# هنا بنمرر الـ delivery_report بشكل منفصل عشان النسخ الجديدة
producer = SerializingProducer(producer_conf)

# 5. دوال التعامل مع Finnhub WebSocket
def on_message(ws, message):
    data = json.loads(message)
    if data['type'] == 'trade':
        for trade in data['data']:
            # تحضير البيانات بناءً على الـ Schema
            payload = {
                "symbol": trade['s'],
                "price": float(trade['p']),
                "timestamp": int(trade['t']),
                "volume": float(trade['v'])
            }
            
            # إرسال البيانات لكافكا (توبيك finnhub_stocks)
            try:
                producer.produce(topic='finnhub_stocks',key=payload['symbol'], value=payload, on_delivery=delivery_report)
                producer.poll(0) # بيخلي الـ Producer يبعت الرسايل فوراً
                print(f"🚀 Sent: {payload['symbol']} @ {payload['price']}")
            except Exception as e:
                print(f"⚠️ Error producing message: {e}")

def on_error(ws, error):
    print(f"❗ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔌 Connection Closed")

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
    # تشغيل الـ WebSocket
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever()