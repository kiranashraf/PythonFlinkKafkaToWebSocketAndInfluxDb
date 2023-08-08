import asyncio
import websockets
import json
from pyflink.datastream import StreamExecutionEnvironment, SinkFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import SinkFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

# Define the WebSocket sending function
async def send_to_websocket(uri, json_data):
    async with websockets.connect(uri) as websocket:
        str_data = str(json_data)
        print(str_data)
        await websocket.send(str(json_data))


# Function to convert a Row to a dictionary
def row_to_dict(row):
    row_dict = {
            'sensor_id': row.f0,
            'sensor_type': row.f1,
            'factory_number': row.f2,
            'event_time': row.f3,
            'temperature_celsius': row.f4
        }
    return row_dict

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/kiran.ashraf/flink-1.17.1/lib/flink-sql-connector-kafka-1.17.1.jar")
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    
    # Register a Kafka source table
    t_env.execute_sql("""
            CREATE TABLE kafka_source_table (
                sensor_id STRING,
                sensor_type STRING,
                factory_number STRING,
                event_time BIGINT,
                temperature_celsius DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'streamTestTopic',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'some_id',
                'scan.startup.mode' = 'latest-offset',  
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
    """)
                        
    # convert the sql table to Table API table
    table = t_env.from_path("kafka_source_table")
    ds = t_env.to_append_stream(table,Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.DOUBLE()]))
    print("DS: ", ds)

    # Define the processing logic using a map transformation
    def process_message(row, uri):
        row_dict = row_to_dict(row)
        json_data = json.dumps(row_dict)
        #print(json_data)
        asyncio.run(send_to_websocket(uri,json_data))
        return json_data

    mapped_messages = ds.map(lambda row: process_message(row, 'ws://localhost:8000'))

    env.execute("Kafka Message Processing")

if __name__ == '__main__':
    main()
