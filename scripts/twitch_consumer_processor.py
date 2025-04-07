import apache_beam as beam
from apache_beam.io.gcp.pubsub import  ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
import json
import os
import time

#google-cloud configuration===START=======================================
project_id = os.environ.get("google_cloud_project_id")
subscription = "twitch_streams_data-sub"
topic = f'projects/{project_id}/subscriptions/{subscription}'
#google-cloud configuration===START=======================================

def run():
    #Define dataflow pipeline options
    options = PipelineOptions(
        runner = 'DataflowRunner',
        project = project_id,
        region = 'us-west1',
        temp_location = "gs://temp-data-0405/",
        staging_location = "gs://staging-bucket-0405/",
        streaming = True,
        #Dataflow optional parameters
        # job_name = "streaming-twitch-data",
        # num_workers = 3,
        # max_num_workers = 10,
        # disk_size_gb = 100,
        # autoscaling_algorithm = 'THROUGHPUT_BASED',
        # machine_type = 'n1-standard-4',
        service_account_email='dataengineering123@thermal-scene-455920-b7.iam.gserviceaccount.com'
    )

    #Define beam pipeline
    with beam.Pipeline(options=options) as pipeline:
        #Read input data from Pub/Sub:
        print(topic)
        messages = pipeline | ReadFromPubSub(subscription=topic)
        print(messages)
        #parse json messages
        parsed_messages = messages | beam.Map(lambda msg: json.loads(msg))
        print(parsed_messages)
        #Extract the required fields from messages
        stream_data = parsed_messages | beam.Map(lambda data:{
            'stream_id': data.get("id", 'N/A'),
            'user_id':data.get('user_id', None),
            'user_name':data.get('user_name', None),
            'game_id':data.get('game_id', None),
            'game_name':data.get('game_name', None),
            'stream_type':data.get('type', None),
            'viewer_count':data.get('viewer_count',None),
            'language':data.get('language',None),
            'started_at':data.get('started_at', None)
        })

        print(stream_data)
        #define schema for the twitch_streams table
        twitch_streams_schema = {
            'fields':[
                {'name':'stream_id', 'type':'STRING'},
                {'name':'user_id', 'type':'STRING'},
                {'name':'user_name', 'type':'STRING'},
                {'name':'game_id', 'type':'STRING'},
                {'name':'game_name', 'type':'STRING'},
                {'name':'stream_type', 'type':'STRING'},
                {'name':'viewer_count', 'type':'INTEGER'},
                {'name':'language', 'type':'STRING'},
                {'name':'started_at', 'type':'TIMESTAMP'},
            ]
        }


        #write the twitch_streams data to BigQuery table
        stream_data | 'Write twitch_streams_data to BigQuery' >> WriteToBigQuery(
            table = 'thermal-scene-455920-b7.twitch.twitch_streams_data',
            schema=twitch_streams_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        print(f"loaded stream to Bigquery: {time.time()}")
if __name__ == "__main__":
    run()
