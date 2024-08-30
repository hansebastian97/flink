import json
import random
import time

from faker import Faker
from datetime import datetime
from kafka.admin import KafkaAdminClient, NewTopic

from kafka import KafkaProducer
# Function for generating dummy data
def generate_transactions(fake):
  return {
    "transaction_id": fake.uuid4(),
    "customer_id": random.randint(1000,10000),
    'transaction_timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
    "merchant": random.choice(["KFC","McDonalds","Cek Min","Sour Sally", "Warung Zaonk", "Soto Semut","Mamank"]),
    "transaction_method": random.choice(["BRI","BCA","BNI","CIMB Niaga","Mandiri","BTN"]),
    "transaction_amt": round(random.uniform(10000, 1000000),2)
  }

def delivery_report(err, msg):
  if err is not None:
      print(f'Message delivery failed: {err}')
  else:
      print(f"Message delivered to {msg.topic} [{msg.partition()}]")


def main():
  fake = Faker()
  topic_name = "merchant_transactions"
  bootstrap_server = '192.168.60.60:9092'
  app = True
  # Create topic if not exist
  try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_server
    )

    topic_list = []
    topic_list.append(NewTopic(name="merchant_transactions", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
  except Exception as E:
    print(E)

  while app == True:
    producer = KafkaProducer(
      bootstrap_servers=bootstrap_server,
      value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    try:
      transaction = generate_transactions(fake)
      print(transaction)
      producer.send(
        topic=topic_name,
        key=transaction['transaction_id'].encode('utf-8'),
        value=transaction
      )
      time.sleep(1)
      producer.flush()    
    except BufferError:
      print("Buffer full! Waiting...")
      time.sleep(1)
    except Exception as E:
       print(E)

if __name__ == '__main__':
  main()