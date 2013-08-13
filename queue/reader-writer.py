#!/usr/bin/env python

import os           #for some reason httplib2 works better w/ this included
import time         #for sleep
import threading    #for threads

from random import randint      #for randomizing queues

from kwmqueues.utils.ainput import ainput             #input gathering
from kwmqueues.utils.tableoutput import TableOutput   #pretty table printing

from queue import QUEUE     #rackspace public queue service

#Thread that picks a random queue every 10 seconds and claims a message
class Reader(threading.Thread):
    def __init__(self, x, num_queues):
        self.x = x                          #thread number
        self.num_queues = num_queues        #number of queues that are available to select
        self.kill_received = False          #signal to die
        self.queue = QUEUE()                #public queue service
        threading.Thread.__init__(self)     #call super
        return

    def run(self):
        while not self.kill_received:
            #pick a random queue
            qnum = randint(0, self.num_queues - 1)
            print "Reader " + str(self.x) + " reading from queue q" + str(qnum)
        
            #claim message from queue
            response, content = self.queue.ClaimMessages("q" + str(qnum), 1)
            if not (response.status == 201 or response.status == 204 or response.status == 200):
                print "Error claiming message " + str(qnum)
                print response
                print content

            #sleep for a bit
            time.sleep(10)

        print "Reader " + str(self.x) + " exiting."
        return  
#Thread that picks a random queue every 10 seconds and writes a message
class Writer(threading.Thread):
    def __init__(self, x, num_queues):
        self.x = x                          #thread number
        self.num_queues = num_queues        #number of queues that are available to select
        self.kill_received = False          #signal to die
        self.queue = QUEUE()                #public queue service
        threading.Thread.__init__(self)     #call super
        return

    def run(self):
        while not self.kill_received:
            #pick a random queue
            qnum = randint(0, self.num_queues - 1)
            print "Writer " + str(self.x) + " writing to queue q" + str(qnum)
            body = "{\"writer_thread\": \"" + str(self.x) + "\"}"

            #write message to queue
            response, content = self.queue.PostMessage("q" + str(qnum), 300, body)
            if not (response.status == 201 or response.status == 204 or response.status == 200):
                print "Error posting message " + str(qnum)
                print response
                print content

            #sleep for a bit
            time.sleep(10)

        print "Writer " + str(self.x) + " exiting."
        return  

#thread that shows table stats
class StatUpdater(threading.Thread):
    def __init__(self):
        self.kill_received = False          #signal to die
        self.queue = QUEUE()                #public queue service
        threading.Thread.__init__(self)     #call super
        return

    def run(self):
        while not self.kill_received:
            #get a list of queues
            print "Retrieving Queue Stats..."
            queues = self.queue.ListQueues()
            labels = ('Name', 'Free Messages', 'Claimed Messages')
            rows = []

            #print the queue details
            for q in queues:
                stats = self.queue.GetQueueStats(str(q['name']))
                rows.append((str(q['name']), str(stats['messages']['free']), str(stats['messages']['claimed'])))
        
            to = TableOutput()
            print to.indent([labels] + rows, hasHeader=True, separateRows=True, prefix='| ', postfix=' |')
            
            #sleep for a bit
            time.sleep(2)

        print "Updater exiting"
        return  

#helper function to create queues
def create_queues(queues):
    queue = QUEUE()
    for x in range(0, queues):
        print "Creating queue q" + str(x)
        response, content = queue.CreateQueue("q" + str(x))
        print response
        print content

    return

def main():

    #keep track of the threads
    reader_threads = []
    writer_threads = []

    #get user input
    num_readers = ainput("Enter the number of readers: ").getInteger()
    num_writers = ainput("Enter the number of writers: ").getInteger()
    num_queues = ainput("Enter the number queues: ").getInteger()

    #create the queues
    create_queues(num_queues)

    #create the readers
    for x in range(num_readers):
        r = Reader(x, num_queues)
        reader_threads.append(r)
        r.daemon = True
        r.start()

    #create the writers
    for x in range(num_writers):
        w = Writer(x, num_queues)
        writer_threads.append(w)
        w.daemon = True
        w.start()

    #start the stats thread
    s = StatUpdater()
    s.daemon = True
    s.start()
    while True:
        try:
            #sleep a little to give other threads cpu
            #keyboard interrput will still break this
            time.sleep(100)
        except KeyboardInterrupt:
            print "Caught interrupt, terminating threads..."
            for t in reader_threads:
                print "Killing reader"
                t.kill_received = True
            for t in writer_threads:
                print "Killing writer"
                t.kill_received = True
            print "Killing Updater"
            s.kill_received = True
            time.sleep(15)
            break
    return
    
if __name__ == '__main__':
    try:
        main()
    except EOFError:
        print "\n"
        exit()
