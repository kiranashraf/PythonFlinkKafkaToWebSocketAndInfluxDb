import asyncio
import websockets
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types
from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision

# InfluxDB connection settings
influxdb_url = "https://eu-central-1-1.aws.cloud2.influxdata.com"
influxdb_token = "jURgAr5aBikkIUmLAihZhWYSjeGv4_Duju5YDyvCaOJCTQ9JMBVt_3K082YFOGxeqhissFxLPx5iCWNsZFYO1g=="
influxdb_org =  "self"
influxdb_bucket = "testBucketForPyflink"

# Define the WebSocket sending function
async def send_to_websocket(uri, data):
    try:
        async with websockets.connect(uri) as websocket:
            #first convert data to json
            json_data = json.dumps(data)
            await websocket.send(json_data)
    except websockets.exceptions.WebSocketException as e:
        print("WebSocket error:", e)
    except Exception as e:
        print("An error occurred:", e)
 
# Function to convert a Row to a dictionary
def row_to_dict(row):
    row_dict = {
            'sensor_id': row.f0,
            'sensor_type': row.f1,
            'factory_number': row.f2,
            'temperature_celsius': row.f3,
            'timestamp': row.f4.strftime('%Y-%m-%d %H:%M:%S.%f')
        }
    return row_dict


# Function to write InfluxDB Point to InfluxDB
def write_to_influxdb(point):
    # Initialize InfluxDB client
    client = InfluxDBClient3(host=influxdb_url, token=influxdb_token, org=influxdb_org)
    client.write(database=influxdb_bucket, record=point)



# Function to create InfluxDB Point
def create_influxdb_point(row):
    return Point("sensor_data") \
        .tag("sensor_id", row.get('sensor_id')) \
        .time(row.get('timestamp'), WritePrecision.MS) \
        .field("sensor_type", row.get('sensor_type')) \
        .field("factory_number", row.get('factory_number')) \
        .field("temperature_celsius", row.get('temperature_celsius'))

# Define the processing logic using a map transformation
def process_message(row, uri):
    row_dict = row_to_dict(row)
    #Send data to Websocket
    asyncio.run(send_to_websocket(uri,row_dict))
    #write data in InfluxDB
    write_to_influxdb(create_influxdb_point(row_dict))
    return row_dict


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/kiran.ashraf/flink-1.17.1/lib/flink-sql-connector-kafka-1.17.1.jar")
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    print("Flink Job Started!")
    
    # Register a Kafka source table
    t_env.execute_sql("""
            CREATE TABLE kafka_source_table (
                sensor_id STRING,
                sensor_type STRING,
                factory_number STRING,
                temperature_celsius DOUBLE,
                `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
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
    ds = t_env.to_append_stream(table,Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]))

    ds.map(lambda row: process_message(row, 'ws://localhost:8000')).print()

    env.execute("Kafka Message Processing")

if __name__ == '__main__':
    main()
