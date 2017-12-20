# Received a whole sentence somehow, split it by MeCab

import MeCab
# for analysis
import storm
# main json formatter
import redis
# directly write to redis db for some data
import logging
import os
from datetime import datetime

class SplitSentenceBolt(storm.BasicBolt):

        

    def process(self, tup):

        f = open("/root/Japanese.txt")
        self.stopwords = f.read().split('\n')
        f.close()
        self.m = MeCab.Tagger("-Ochasen")
        r = redis.Redis(host='localhost', port=6379, db=0)
        

        pid = os.getpid()
        base_path = '/var/log/takatoshi/'
        logging.basicConfig(filename=base_path+__file__+'.log', level=logging.DEBUG)
        logging.debug(datetime.now())
        logging.debug("abs path of py file: " + os.path.abspath(__file__))

        sentence = tup.values[0]
        ts = tup.values[1]

        res = self.m.parse(sentence).splitlines()[:-1]

        output = set()

        for _ in res:
            fa = _.split('\t')
            if u"名詞" in fa[3] and not fa[0] in self.stopwords:
                output.add(fa[0])

        if len(output):
            r.incr('total_'+str(ts))
            for _ in output:
                logging.debug("getting noun : " + _)
                storm.emit([_,ts])

        

SplitSentenceBolt().run()
