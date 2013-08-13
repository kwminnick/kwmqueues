#!/usr/bin/env python

import os
from pprint import pprint

from kwmqueues.utils.ainput import ainput
from kwmqueues.utils.tableoutput import TableOutput

from queue import QUEUE

class CloudQueueCLI:         
    def __init__(self):
        return
    
    def ListQueues(self):         
        queue = QUEUE()
        queues = queue.ListQueues()
        labels = ('Name', 'href', 'metadata')
        rows = []

        for queue in queues:
            rows.append((str(queue['name']), str(queue['href']), str(queue['metadata'])))
        
        to = TableOutput()
        print to.indent([labels] + rows, hasHeader=True, separateRows=True, prefix='| ', postfix=' |')

        return

    def GetQueueStats(self):
        name = ainput("Queue Name: ").getString()

        queue = QUEUE()
        stats = queue.GetQueueStats(name)
        labels = ('Queue', 'Free', 'Claimed')
        rows = []

        rows.append((name, str(stats['messages']['free']), str(stats['messages']['claimed'])))
        
        to = TableOutput()
        print to.indent([labels] + rows, hasHeader=True, separateRows=True, prefix='| ', postfix=' |')
    
        return

    def CreateQueue(self):
        name = ainput("Queue Name: ").getString()

        queue = QUEUE()
        response, content = queue.CreateQueue(name)
        print response
        print content

        return

    def DeleteQueue(self):
        name = ainput("Queue Name: ").getString()

        queue = QUEUE()
        response, content = queue.DeleteQueue(name)
        print response
        print content

        return

    def PostMessage(self):
        qname = ainput("Queue Name: ").getString()
        ttl = ainput("TTL: ").getInteger()
        body = ainput("Body: ").getString()

        queue = QUEUE()
        response, content = queue.PostMessage(qname, ttl, body)
        print response
        print content

        return

    def GetMessages(self):
        qname = ainput("Queue Name: ").getString()

        queue = QUEUE()
        response, content = queue.GetMessages(qname)
        print response
        print content

        return

    def DeleteMessage(self):
        qname = ainput("Queue Name: ").getString()
        message_id = ainput("Message Id: ").getString()

        queue = QUEUE()
        response, content = queue.DeleteMessage(qname, message_id)
        print response
        print content

        return

    def ClaimMessages(self):
        qname = ainput("Queue Name: ").getString()
        num_messages = ainput("Number of Messages: ").getInteger()

        queue = QUEUE()
        response, content = queue.ClaimMessages(qname, str(num_messages))
        print response
        print content

        return
   
def show_main_menu():
    print "\nRackspace Queuing\n"
    qcli = CloudQueueCLI()        
    
    print "\nChoose from an option below:\n\
        1. List Queues\n\
        2. Create Queue\n\
        3. Delete Queue\n\
        4. Post Message\n\
        5. Get Messages\n\
        6. Delete Message\n\
        7. Get Queue Stats\n\
        8. Claim Messages\n\
        9. Exit\n"

    option = ainput("Select a number from the menu above: ").getString()
    arguments = []
    
    if option == '1':
        qcli.ListQueues()
    elif option == '2':
        qcli.CreateQueue()
    elif option == '3':
        qcli.DeleteQueue()
    elif option == '4':
        qcli.PostMessage()
    elif option == '5':
        qcli.GetMessages()
    elif option == '6':
        qcli.DeleteMessage()
    elif option == '7':
        qcli.GetQueueStats()
    elif option == '8':
        qcli.ClaimMessages()
    elif option == '9':
        print "\n"
        exit()
                    
    return


def main():
    
    while True:
        os.system('clear')
        show_main_menu()
        raw_input("Press any key to continue")
        
    return
    
if __name__ == '__main__':
    try:
        main()
    except EOFError:
        print "\n"
        exit()
