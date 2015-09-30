#!/usr/bin/env python
import sys
import httplib2
import base64
import ConfigParser
import csv
import json
import time
import datetime

config = ConfigParser.ConfigParser()

from apiclient import discovery
from oauth2client import client as oauth2client

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']
PROJECT_ID = 'bledata-1078'
message1 = base64.b64encode('Hello Cloud Pub/Sub')
message2 = base64.b64encode('We are on this boat')

#Create a POST body fo the pub/sub request
body = {
        'messages':[
            {'data': message1},
            {'data': message2},
        ]
}


def create_pubsub_client(http=None):
    credentials = oauth2client.GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)
    print "done authorization"
    return discovery.build('pubsub', 'v1', http=http)

def main():
    print "Hello, in Main"
    config.read(sys.argv[1]);

    loc_id = config.getint('Default', 'location_id')
    agg_id =  config.getint('Default', 'aggregator_id')
    proj_id = config.get('Default', 'project_id', 0)
    topic_str = config.get('Default', 'topic_str', 0)
    rssi_file = config.get('Default', 'rssi_file', 0)

    client = create_pubsub_client()
    #create topic only the first time
    topic_name = 'projects/' + proj_id + '/topics/' + topic_str

    print "Topic Name = " + topic_name
    print "RSSI Vals in file " + rssi_file

    #topic = client.projects().topics().create(
    #        name=topic_name, body={}).execute()
    #print 'Created %s' % topic.get('name')

    FieldNames = ("Location ID", "Agggregator ID", "Timestamp", "BDADDR", "BDADDRType", "Device Type", "RSSI")
    f = open(rssi_file, 'rb')

    body1 = {}
    messages = []

    reader = csv.DictReader(f, FieldNames)

    for row in reader:
        print row
        json_data = json.dumps(row)
        encoded_msg = base64.b64encode(str(row))
        message= {}
        message = {"data":encoded_msg}
        messages.append(message)

       # json.dump(row, fj)
       # print json_data

    body1['messages'] = messages

    print json.dumps(body1, indent=4)


    resp = client.projects().topics().publish(
            topic=topic_name, body=body1).execute()

    f.close()


    message_ids = resp.get('messageIds')
    if message_ids:
        for message_id in message_ids:
            #Process each msg
            print message_id

if __name__ == '__main__':
    main()

