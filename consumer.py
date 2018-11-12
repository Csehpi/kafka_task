from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'liligo',
     bootstrap_servers=['127.0.0.1:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

print("Start")

count_dict = dict()

for message in consumer:
    if message.key not in count_dict:
        count_dict[message.key] = message.value
    else:
        count_dict[message.key] += message.value
        
print(count_dict)