import logging
import os 
import urllib

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col



def main():
  # Configure logging
  logging.basicConfig(level=logging.INFO)
  logger = logging.getLogger(__name__)

  try:
    # Create Stream TableEnvironment to execute the queries
    env = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env)
    topic_name = "merchant_transactions"

    # Add Jars
    kafka_jar  = urllib.parse.quote(os.path.abspath(os.path.join(os.getcwd(), 'flink_jars','flink-sql-connector-kafka-3.2.0-1.19.jar')))
    postgre_jar = urllib.parse.quote(os.path.abspath(os.path.join(os.getcwd(), 'flink_jars','postgresql-42.6.2.jar')))
    jdbc_jar = urllib.parse.quote(os.path.abspath(os.path.join(os.getcwd(), 'flink_jars','flink-connector-jdbc-3.2.0-1.19.jar')))
    
    table_env.get_config().set('pipeline.jars', 
      "file:///{};file:///{};file:///{}".format(kafka_jar, postgre_jar, jdbc_jar)
    )

    # Get data from Kafka Producer
    source_ddl = """
            CREATE TABLE source_table(
              transaction_id VARCHAR,
              customer_id INT,
              transaction_timestamp VARCHAR,
              merchant VARCHAR,
              transaction_method VARCHAR,
              transaction_amt FLOAT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'merchant_transactions',
              'properties.bootstrap.servers' = 'http://192.168.60.60:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    # Create table in PostgreSQL
    sink_table = """
        CREATE TABLE IF NOT EXISTS sink_table (
            merchant STRING,
            `total_amt` FLOAT,
            PRIMARY KEY (merchant) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://192.168.60.60:5432/flink',
            'table-name' = 'transaction_agg',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
# 'driver' = 'com.mysql.cj.jdbc.Driver'
    sink_query = """
    INSERT INTO sink_table
    SELECT merchant, SUM(transaction_amt) AS total_amt
    FROM source_table
    GROUP BY merchant
    """
    # Execute query source DDL.wait()
    table_env.execute_sql(source_ddl)

    # Execute query create Sink Table
    table_env.execute_sql(sink_table)
    # Execute query insert into sink_table
    table_env.execute_sql(sink_query).wait()



  except Exception as E:
    logger.error(f"Error in Flink Kafka Consumer Job: {E}")

if __name__ == "__main__":
  main()