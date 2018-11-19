# -*- coding: utf-8 -*-
import time
import json
import socket

import arrow
#from concurrent.futures import ThreadPoolExecutor as Pool
from concurrent.futures import ProcessPoolExecutor as Pool

from helper_consul import ConsulAPI
from helper_kafka_consumer import KafkaConsumer
from helper_jcbk import JCBK
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()

        # request方法类
        self.kc = None
        self.jc = JCBK(**dict(self.my_ini['jcbk']))
        self.con = ConsulAPI()
        self.con.path = dict(self.my_ini['consul'])['path']

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

        self.item = None
        self.part_list = list(range(60))
        self.workers = dict(self.my_ini['sys'])['workers']
        #self.pool = Pool(max_workers=dict(self.my_ini['sys'])['workers'])
        #self.pool2 = Pool(max_workers=32)

    def upload_data(self):
        now = arrow.now('PRC').timestamp
        items = []
        offsets = {}
        t1 = time.time()
        pool = Pool(max_workers=self.workers)
        for i in range(400):
            msg = self.kc.c.poll(0.003)
        
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                m = json.loads(msg.value().decode('utf-8'))['message']
                #jgsj = arrow.get(m['jgsj']).replace(hours=-8).to('PRC').timestamp
                #if (now-jgsj) < 300:
                #    items.append(m)
                obj = pool.submit(self.jc.post_jcbk, m)
                items.append(obj)
            par = msg.partition()
            off = msg.offset()
            offsets[par] = off
        if offsets == {}:
            return 0
        else:
            print('items={0}'.format(len(items)))
            logger.info('items={0}'.format(len(items)))
            #t1 = time.time()
            pool.shutdown()
            t2 = time.time()
            print('t=%.3s'%(t2-t1))
            self.kc.c.commit(async=False)
            #print(offsets)
            logger.info(offsets)
            return 1

    def main_loop(self):
        count = 0
        while 1:
            try:
                if self.kc is None:
                    self.kc = KafkaConsumer(**dict(self.my_ini['kafka']))
                    self.kc.assign(self.part_list)
                n = self.upload_data()
                if n > 0:
                    count = 0
                elif count > 3:
                    logger.warning('exit')
                    exit()
                else:
                    count += 1
                    logger.warning('count={0}'.format(count))
                    time.sleep(1)
            except Exception as e:
                logger.exception(e)
                count += 1
                time.sleep(15)

        
