import json

from kwmqueues.constants.auth import AuthInfo
from queueapi import QUEUEAPI

class QUEUE(object):
    def __init__(self):
        auth = AuthInfo()
        self.xmlns = auth.QUEUE_XMLNS
        self.queueapi = QUEUEAPI()
        return

    def ListQueues(self):
        queuesList = []
        url = self.queueapi.URL_QUEUES + "?detailed=true"
        response, content = self.queueapi.RequestURL(url)
        if response.status == 200:
            queues = json.loads(str(content))
                
            if 'queues' in queues:
                queueList = queues['queues']
        elif response.status == 204:
            queueList = []
        return queueList

    def GetQueueStats(self, name):
        url = self.queueapi.URL_QUEUES + "/" + name + "/stats"
        response, content = self.queueapi.RequestURL(url)
        stats = json.loads(str(content))
        return stats

    def CreateQueue(self, name):
        url = self.queueapi.URL_QUEUES + "/" + name

        #create some fake meta-data for now (seems to be required)
        #TODO ask via cli
        metadata = '{}'
        #metadata = '{"metadata": "Awesome Queue"}'
        response, content = self.queueapi.PutURL(url, metadata)
        return response, content

    def DeleteQueue(self, name):
        url = self.queueapi.URL_QUEUES + "/" + name

        response, content = self.queueapi.DeleteURL(url)
        return response, content

    def PostMessage(self, qname, ttl, body):
        url = self.queueapi.URL_QUEUES + "/" + qname + "/messages"

        json = "[{ \"ttl\": " + str(ttl) + ", \"body\": " + body + "}]"
        response, content = self.queueapi.PostURL(url, json)
        return response, content

    def GetMessages(self, qname):
        url = self.queueapi.URL_QUEUES + "/" + qname + "/messages?echo=true"

        response, content = self.queueapi.RequestURL(url)
        return response, content

    def DeleteMessage(self, qname, message_id):
        url = self.queueapi.URL_QUEUES + "/" + qname + "/messages/" + message_id

        response, content = self.queueapi.DeleteURL(url)
        return response, content

    def ClaimMessages(self, qname, num_messages):
        url = self.queueapi.URL_QUEUES + "/" + qname + "/claims?limit=" + str(num_messages)
        json = "{ \"ttl\": 300, \"grace\": 300 }"
        response, content = self.queueapi.PostURL(url, json)
        return response, content
