import logging
import os 
import urllib

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema


def main():
  # Configure logging
  logging.basicConfig(level=logging.INFO)
  logger = logging.getLogger(__name__)

  try:
    env = StreamExecutionEnvironment.get_execution_environment()

    # kafka_connector  = os.path.join(os.getcwd(), 'flink_jars','flink-sql-connector-kafka-3.2.0-1.19.jar')
    # kafka_client = os.path.join(os.getcwd(), 'flink_jars','kafka-clients-3.7.1.jar')
    kafka_connector  = os.path.abspath(os.path.join(os.getcwd(), 'flink_jars','flink-sql-connector-kafka-3.2.0-1.19.jar'))
    kafka_client = os.path.abspath(os.path.join(os.getcwd(), 'flink_jars','flink-sql-connector-kafka-3.2.0-1.19.jar'))


    env.add_jars(
      str("file:///"+ urllib.parse.quote(kafka_connector)),
      str("file:///"+ urllib.parse.quote(kafka_client))
    )


    topic_name = "merchant_transactions"
    kafka_consumer = FlinkKafkaConsumer(
        topics=topic_name,
        properties={'bootstrap.servers': 'http://192.168.60.60:9092',
                    'group.id': 'flink-group',
                    'auto.offset.reset': 'earliest'},
        deserialization_schema= SimpleStringSchema()
    )

    stream = env.add_source(kafka_consumer)
    stream.print()
    
    env.execute("Flink Kafka Consumer Example")
  except Exception as E:
    logger.error(f"Error in Flink Kafka Consumer Job: {E}")

if __name__ == "__main__":
  main()