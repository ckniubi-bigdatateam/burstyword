import logging

from datetime import datetime
import sys
import os
import time

import storm

import redis

from time import sleep

class PawooSentenceSpout(storm.Spout):
    # do nothing on ack and fail signals
    
        

        

    def nextTuple(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        self.ts = 1

        pid = os.getpid()
        base_path = '/var/log/takatoshi/'
        logging.basicConfig(filename=base_path+__file__+'.log', level=logging.DEBUG)
        logging.debug(datetime.now())
        logging.debug("abs path of py file: " + os.path.abspath(__file__))
        item = r.rpop('msgpool')
        tw = r.get('timewindow')
        tw = int(float(str(tw)[2:-1]))
        tmp1 = r.get('twno')
        self.ts = int(float(str(tmp1)[2:-1]))
        while item is None:
            sleep(0.5)
            item = r.rpop('msgpool')
            
        '''if time.time() - self.start_time > 300:
            self.ts = self.ts + 1
            self.start_time = time.time()'''
        logging.debug("emitting sentence" + item.decode('utf-8'))

        storm.emit([item.decode('utf-8'),self.ts])

PawooSentenceSpout().run()
