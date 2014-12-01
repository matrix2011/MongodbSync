#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import logging
from pymongo import MongoClient
import traceback
import replica_syn
import utils
from utils import MongoConnInfo, SyncConfig

reload(sys)
sys.setdefaultencoding('utf-8')

''' init the logging '''
if len(sys.argv) < 3:
    print 'Usage: config-file, log-file, [is-debug]'
    sys.exit(1)

g_conf_file = sys.argv[1]
g_log_file = sys.argv[2]

is_debug = True if len(sys.argv) == 4 and sys.argv[3] == 'true' else False

logging.basicConfig(filename=g_log_file, format='[%(levelname)s] %(asctime)s %(module)s:%(funcName)s:%(lineno)d %(message)s',
                    level=logging.INFO if not is_debug else logging.DEBUG)
logging.info("config file: %s, log file: %s, is_debug: %s" % (g_conf_file, g_log_file, is_debug))


class MongodSynchronizer(object):
    def __init__(self, conf):
        '''
        :param conf: the config of this synchronizer
        :return:
        '''
        # 首先需要判断 连接的是 mongos 还是 mongod 副本集
        # 1. 如果都不是会抛出异常，退出程序；
        # 2. 获取副本集合的信息，放入到数组对象维护
        # 遍历数组，读取其中的 oplog 信息，写入到目的 mongo 中

        # config, for sync
        self._conf = conf
        # source mongodb conns
        self._src_mongo_conns = []

        #
        src_mc = MongoClient(conf.mongo_src[0])
        if conf.mongo_src[1] and conf.mongo_src[2]:
            src_mc.admin.authenticate(conf.mongo_src[1], conf.mongo_src[2])

        src_addrs = []
        if src_mc.is_mongos:
            shards = self._getShards(src_mc)
            src_addrs.extend(shards)
        else:
            repl = self._getReplInfo(src_mc)
            src_addrs.append(repl)

        # src conn
        for src_addr in src_addrs:
            self._src_mongo_conns.append(MongoConnInfo(src_addr[0], conf.mongo_src[1], conf.mongo_src[2], src_addr[1]))

        src_mc.close()

        # dest mongodb conns
        self._dest_mongo_conn = MongoConnInfo(conf.mongo_dest[0], conf.mongo_dest[1], conf.mongo_dest[2])

        # logging
        for src_addr in self._src_mongo_conns:
            logging.info("source mongo conn: %s" % (src_addr))
        logging.info("dest mongo conn: %s" % (self._dest_mongo_conn))

    def _getReplInfo(self, client):
        """ Get replica set information: (addr, repl name) """
        status = client.admin.command({'replSetGetStatus': 1})

        if status['ok'] == 1:
            repl = status['set']
            members = ','.join([member['name'] for member in status['members']])

            return (members, repl)
        else:
            logging.info("stauts not ok: %s" % (status))

        return (None, None)

    def _getShards(self, client):
        '''
        :param client:
        :return:
        '''
        shards = []

        for shard in client["config"]["shards"].find():
            shards.append((shard["host"].split('/')[1], shard["_id"]))

        return shards

    def startSync(self):
        ''' 对每一个源，起一个线程进行同步 '''
        threads = []
        for src_conn in self._src_mongo_conns:
            t = replica_syn.ReplicaSynchronizer(src_conn, self._dest_mongo_conn, self._conf)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()


if __name__ == '__main__':
    conf = SyncConfig(g_conf_file)

    if not conf.parse():
        logging.error("config parse failed.")
        sys.exit(1)

    logging.info("begin sync...")

    try:
        sync = MongodSynchronizer(conf)
        sync.startSync()
    except:
        logging.error("mongo synchronizer exception: %s" % (traceback.format_exc()))

    logging.info("system exit.")