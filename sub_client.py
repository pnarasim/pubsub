#!/usr/bin/env python

import sys
import httplib2
import base64
import ConfigParser

# You can fetch multiple messages with a single API call.
batch_size = 100

# Create a POST body for the Pub/Sub request
body_pull = {
    # Setting ReturnImmediately to false instructs the API to wait
    # to collect the message up to the size of MaxEvents, or until
    # the timeout.
    'returnImmediately': False,
    'maxMessages': batch_size,
}

config = ConfigParser.ConfigParser()

from apiclient import discovery
from oauth2client import client as oauth2client

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']

def create_pubsub_client(http=None):
    credentials = oauth2client.GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)

    return discovery.build('pubsub', 'v1', http=http)

def main():
    print "Hello, in Sub Main"
    config.read(sys.argv[1]);

    loc_id = config.getint('Default', 'location_id')
    agg_id =  config.getint('Default', 'aggregator_id')
    proj_id = config.get('Default', 'project_id', 0)
    topic_str = config.get('Default', 'topic_str', 0)
    sub_id = config.get('Default', 'subscription_id', 0)

    sub_name = 'projects/' + proj_id + '/subscriptions/' + sub_id
    topic_name = 'projects/' + proj_id + '/topics/' + topic_str

    print "Topic = " + topic_name + "\nSub ID = " + sub_name

    client = create_pubsub_client()
    # Create a POST body for the Pub/Sub request
    body = {
        # The name of the topic from which this subscription receives messages
        'topic': topic_name,
    }
    #create subscription only first time
    #subscription = client.projects().subscriptions().create(
     #               name= sub_name, body=body).execute()

    #print 'Created: %s' % subscription.get('name')




    while True:

        resp = client.projects().subscriptions().pull(
            subscription=sub_name, body=body_pull).execute()

        print "Resp received"

        received_messages = resp.get('receivedMessages')
        if received_messages is not None:
            ack_ids = []
            for received_message in received_messages:
                pubsub_message = received_message.get('message')
                if pubsub_message:
                    # Process messages
                    print base64.b64decode(str(pubsub_message.get('data')))
                    # Get the message's ack ID
                    ack_ids.append(received_message.get('ackId'))

            # Create a POST body for the acknowledge request
            ack_body = {'ackIds': ack_ids}

            # Acknowledge the message.
            client.projects().subscriptions().acknowledge(
                subscription=sub_name, body=ack_body).execute()

if __name__ == '__main__':
    main()

