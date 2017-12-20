import storm
# main json formatter
import redis
# directly write to redis db for some data
import math
from scipy.misc import comb
import sys
from datetime import datetime
import logging
import os
import struct

# redis word data entry format : 
# A sorted set that stores Pj
# A sorted set that saves the word's numbers by the last update:
#  - Last updated, a timestamp
#  - n(i,j), real number
#  - n'(i,j), generated number where N is normalized to 100, po * 100
#  - po(i,j)
#  - pg(i,j)
#  - pb(i,j)
# A sorted set that saves Ni (updated in the upper bolt)

class RedisIOBolt(storm.BasicBolt):

    def getpb(self, pj, k, pg):
        decisionpoint = math.floor(101 * pj)
        if k < decisionpoint or pj == 1 or pj == 0:
            return 0.0
        sth = comb(100,decisionpoint) * (pj ** decisionpoint) * ((1 - pj) ** (100 - decisionpoint))
        return 1 - pg / sth

    def convf(self, x):
        if x is None:
            return 0.0
        return float(str(x)[2:-1])
    
    def process(self, tup):

        conv = self.convf

        r = redis.Redis(host='localhost', port=6379, db=0)

        pid = os.getpid()
        base_path = '/var/log/takatoshi/'
        logging.basicConfig(filename=base_path+__file__+'.log', level=logging.DEBUG)
        logging.debug(datetime.now())
        logging.debug("abs path of py file: " + os.path.abspath(__file__))

        word = tup.values[0]
        ts = tup.values[1]

        logging.debug("received : " + word + " at time window " + str(ts))

        t = r.get("lastupd_"+word)
        if not t is None:
            t = int(float(str(t)[2:-1]))

        logging.debug("updating word data : " + word)

        if t is None:
            t = 0
            logging.debug("t is 0 " + word)
        
        if t > ts:
            logging.debug("should not happen")
            ts = t

        if t < ts:
            oldpj = 0
            if t > 0:
                oldpo = conv(r.get("po_"+word))
                oldpj = conv(r.get("pj_"+word))
                oldpj = oldpj + ( conv(r.get("n_"+word)) / conv(r.get("total_"+str(t))) - oldpo ) / t
                tmp1 = math.floor(100 * conv(r.get("n_"+word)) / conv(r.get("total_"+str(t))))
                pg = comb(100,tmp1) * (oldpj ** tmp1) * ((1 - oldpj) ** (100 - tmp1))
                r.getset("pg_"+word, pg)
                pb = self.getpb(oldpj, tmp1, pg)
                r.getset("pb_"+word, pb)
                # First correct oldpj.

            r.getset("lastupd_"+word, ts)
            r.getset("n_"+word, 1)
            
            tmp2 = r.get("total_"+str(ts))
            
            newpo =  1.0 / conv(tmp2)
            assert newpo <= 1.0, 'error on newpo {0}, word is {1}, t is {2}, ts is {3}'.format(newpo, word, t, ts)
            r.getset("po_"+word, newpo)
            #oldpj = conv(r.get("pj_"+word))
            newpj = t / ts * oldpj + 1 / ts * newpo
            assert newpj <= 1.0, 'error on newpj {0}, word is {1}'.format(newpj, word)
            r.getset("pj_"+word, newpj)
            #tmp1 = math.floor(100 * newpo)
            #newpg = comb(100,tmp1) * (newpj ** tmp1) * ((1 - newpj) ** (100 - tmp1))
            #assert newpg <= 1.0, 'error on newpg {0}, word is {1}, t is {2}, ts is {3}, newpj is {4}, tmp1 is {5}, newpo is {6}'.format(newpg, word, t, ts, newpj, tmp1, newpo) + str(datetime.now())
            #r.getset("pg_"+word, newpg)
            #newpb = self.getpb(newpj, tmp1, newpg)
            #r.getset("pb_"+word, newpb)
            

        else :
            r.incr("n_"+word, 1)
            newpo =  conv(r.get("n_"+word)) / conv(r.get("total_"+str(ts)))
            assert newpo <= 1.0, 'error on newpo {0}, word is {1}'.format(newpo, word)
            oldpo = conv(r.getset("po_"+word, newpo))
            oldpj = conv(r.get("pj_"+word))
            newpj = oldpj - 1 /ts * oldpo + 1 / ts * newpo
            assert newpj <= 1.0 and newpj >= 0.0, 'error on newpj {0}, word is {1}, oldpj is {2}, oldpo is {3}, newpo is {4}'.format(newpj, word, oldpj, oldpo, newpo)
            r.getset("pj_"+word, newpj)
            #tmp1 = math.floor(100 * newpo)
            #tmp2 = int(conv(r.get("total_"+str(ts))))
            #newpg = comb(100,tmp1) * (newpj ** tmp1) * ((1 - newpj) ** (100 - tmp1))
            #assert newpg <= 1.0, 'error on newpg {0}, word is {1}, t is {2}, ts is {3}, newpj is {4}, tmp1 is {5}, newpo is {6}'.format(newpg, word, t, ts, newpj, tmp1, newpo) + str(datetime.now())
            #r.getset("pg_"+word, newpg)
            #newpb = self.getpb(newpj, tmp1, newpg)
            #r.getset("pb_"+word, newpb)
        
        #logging.debug("done")
        storm.emit(["done"])



RedisIOBolt().run()

