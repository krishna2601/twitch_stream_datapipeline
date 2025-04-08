# from google.cloud import pubsub_v1
from google.cloud import secretmanager
import os
import json
import time
import requests
from kafka import KafkaProducer

#Publisher object
# publisher = pubsub_v1.PublisherClient()
kafka_broker ='34.105.66.112:9092'
kafka_topic= 'twitch_streams_data'
# Adding timeout and retries to the producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    acks='all',  # Ensure message is acknowledged by all brokers (can be 'all', 1, or 0)
    retries=5,   # Number of retries on failure
    request_timeout_ms=30000,  # Request timeout (30 seconds)
    reconnect_backoff_ms=1000,  # Delay between reconnect attempts (1 second)
    metadata_max_age_ms=300000,  # Metadata refresh timeout (5 minutes)
    max_in_flight_requests_per_connection=5  # Number of requests allowed in flight
)
# producer = KafkaProducer(bootstrap_servers=[kafka_broker])

#google-cloud configuration===START=======================================
project_id = os.environ.get("google_cloud_project_id")
version_id = "latest"
#google-cloud configuration===END=========================================

def get_gsecret_client():

    # Create the Secret Manager client.
    try:
        client = secretmanager.SecretManagerServiceClient()
    except Exception as e:
        print(f"error occured while calling SecretManagerServiceClient : {e}")
    return client


def get_secret_json(client,secret_name):
    if client is None or secret_name == "":
        print("function client_id_and_secret doesn't have sufficient data/object to proceed further, either client or secret_name is missing.")
        return None
    secret_json = None
    try:
        secret_response = client.access_secret_version(request={"name": secret_name})
        secret = secret_response.payload.data.decode("UTF-8")

        # convert secret string to python dictionary
        secret_json = json.loads(secret)
    except Exception as e:
        print(f"Error occured while retrieving secret json: {e}")

    return secret_json




def get_twitch_oauth_token(url, client_id, client_secret):
    if url == "" or client_id == "" or client_secret == "":
        print("Function get_twitch_oauth_token is missing required parameters. Please provide the non-empty values for the parameters.")
        return None
    try:
        #request the OAuth token
        response = requests.post(url, \
                                 params= {'client_id':client_id, 'client_secret':client_secret, 'grant_type':'client_credentials'})

        #extract access token
        if response.status_code == 200:
            access_token = response.json()["access_token"]
            print(f"access_token: {access_token}")
            return access_token
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception occured while retrieving authorization token for twitch: {e}")
        return None



def get_and_produce_live_streamers(access_token, client_id, url, topic):
    if access_token is None or client_id == "" or url == "":
        print("Function get_and_produce_live_streamers is missing required parameters.")
        exit()
    try:
            streams_url = url
            headers = {
                'Client-ID': client_id,
                'Authorization': f'Bearer {access_token}'
            }
            params = {
                'first': 100
            }
            # keep track of seen ids
            seen_data = set()
            main_count = 0
            while True:
                # unfold the response object
                response = requests.get(streams_url, headers=headers, params=params)
                # response contains all the streamers info on a given page(maximum size is 100)
                streams = response.json()

                if 'data' in streams:
                    count = 0
                    # iterate through all the streamer data
                    for stream in streams['data']:
                        # if id is not a part of the ids in seen_data and then produce data to topic
                        if 'id' in stream and stream['id'] not in seen_data:
                            count += 1
                            seen_data.add(stream['id'])
                            #Extract fields required for streaming===============================
                            data = {}
                            data['id'] = stream['id']
                            data['user_id'] = stream['user_id']
                            data['user_name'] = stream['user_name']
                            data['game_id'] = stream['game_id']
                            data['game_name'] = stream['game_name']
                            data['type'] = stream['type']
                            data['viewer_count'] = stream['viewer_count']
                            data['language'] = stream['language']
                            data['started_at'] = stream['started_at']
                            #===============================================================
                            print(data)
                            #publish data to topic============================================
                            # future = publisher.publish(topic=topic, data=json.dumps(data).encode('utf-8'))
                            # future.result()
                            producer.send(kafka_topic, value=json.dumps(data).encode('utf-8'))
                            # ================================================================
                            #sleep for 1 second before publishing next messages
                            # time.sleep(1)

                        else:
                            continue #if id is already published to topic then skip publishing it and continue to next stream
                        # print(f"successfully published data : {data} to {topic}")
                        print(f"successfully published data : {data} to {kafka_topic}")

                # forward pagination - to move to next page
                if 'pagination' in streams and 'cursor' in streams['pagination']:
                    params['after'] = streams['pagination']['cursor']
                else:
                    break

                main_count += count
                print(
                    f"sent total {count} streamers for current stream and total of {main_count} streamers till now to topic {kafka_topic}")
                # sleep for 30 seconds before retrieving next page
                time.sleep(30)
    except Exception as e:
        print(f"Error occured while retrieving twitch stream data/publishing the data: {e}")

# # google-cloud secret manager configuration====START=====================================
# secret_id = "google-cloud-secret"
# version_id = "latest"
# # google-cloud secret manager configuration====END=====================================
# google_secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
# # Access the secret version for google-cloud.
# response = client.access_secret_version(request={"name": google_secret_name})
# gcloud_secret = response.payload.data.decode("UTF-8")




if __name__ == "__main__":
    secret_client = get_gsecret_client()
    if secret_client is None:
        exit()
    #access secret for twitch
    # Access the secret version for twitch
    twitch_secret_id = "twitch"
    twitch_secret_name = f"projects/{project_id}/secrets/{twitch_secret_id}/versions/{version_id}"
    twitch_secret_json = get_secret_json(secret_client, twitch_secret_name)

    if twitch_secret_json is None:
        exit()
    # access twitch client_id and twitch_secret_json

    twitch_client_id = twitch_secret_json["twitch_client_id"]
    twitch_client_secret = twitch_secret_json["twitch_client_secret"]

    twitch_oauth_url = "https://id.twitch.tv/oauth2/token"
    twitch_access_token = get_twitch_oauth_token(twitch_oauth_url, twitch_client_id,twitch_client_secret)

    if twitch_access_token is None:
        exit()

    twitch_main_url = "https://api.twitch.tv/helix/streams"

    #publish the stream to topic
    try:
        stream_topic = "twitch_streams_data"
        topic = f'projects/{project_id}/topics/{stream_topic}'
        get_and_produce_live_streamers(twitch_access_token,twitch_client_id,twitch_main_url, topic)
    except Exception as e:
        print(f"Error occured while formatting topic name/in get_and_produce_live_streamers(): {e}")
        exit()






