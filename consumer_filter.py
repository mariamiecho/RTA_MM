from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if message.value['amount'] > 1000:
        print("ALERT")
    else: 
        print(message.value)
    
# TWÓJ KOD
# Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT
