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
        

        storm.emit([u"好き",1])

PawooSentenceSpout().run()
