#! /usr/bin/env python
# -*- coding: utf-8 -*-

import time
import threading
import logging
import json
import os
import traceback
import pymongo
import Queue
from threading import Thread
from bson.timestamp import Timestamp

flock = threading.Lock()

g_msg_queue = Queue.Queue(20000)
g_realTS = None


def getFileContent(filename):
    line = None
    try:
        f = open(filename, 'r')
        line = f.readline()
        f.close()
    except:
        logging.error("get file opttime exception: %s" % (traceback.format_exc()))

    return line


def getFileOptTime(filename, repl):
    ''' 根据副本名称和文件名称，得到 上次更新的时间
    :param filename:
    :param repl:
    :return:
    '''
    global flock

    flock.acquire()

    try:
        line = getFileContent(filename)

        if not line:
            return (0, 0)

        json_opttime = json.loads(line)
        repl_info = json_opttime.get(repl)

        if repl_info:
            return (repl_info.get('time', 0), repl_info.get('inc', 0))
    except:
        logging.error("get file opttime exception: %s" % (traceback.format_exc()))
    finally:
        logging.info("release lock in get file opttime")
        flock.release()

    ''' 如果没有同步时间记录，默认是同步一个小时前的数据 '''
    return ((int)(time.time()) - 3600, 0)


def updateOptTime(filename, repl, timeStamp):
    global flock

    flock.acquire()
    line = getFileContent(filename)

    try:
        f = open(filename, 'w')

        if line:
            json_opttime = json.loads(line)
        else:
            json_opttime = {}

        if not json_opttime.has_key(repl):
            json_opttime[repl] = {}

        json_opttime[repl].update({'time': timeStamp.time, 'inc': timeStamp.inc})

        f.write(json.dumps(json_opttime))

        f.close()
    except:
        logging.error("update file opttime exception: %s" % (traceback.format_exc()))
    finally:
        logging.info("release lock in update opt time")
        flock.release()


class ReplicaSynchronizer(threading.Thread):
    """ MongoDB synchronizer"""

    def __init__(self, src_conn, dest_conn, conf):
        global g_msg_queue, g_realTS
        """ Constructor.
        """
        threading.Thread.__init__(self)

        self._src_conn = src_conn
        self._dest_conn = dest_conn
        self._conf = conf

        self._src_mc = self._src_conn.getConn()
        self._dest_mc = self._dest_conn.getConn()

        if self._conf.all_dbs:
            self._conf.dbs = self._src_mc.database_names()
        self._conf.dbs = filter(lambda x: x.split('.')[0] not in ['admin', 'config', 'local'], self._conf.dbs)

        self._ts = None

        # 记录目前为止同步的数据量
        self._syncData = {}

        logging.info("sync init, src(%s) -> dest(%s), dbs: %s" % (
            src_conn, dest_conn, self._conf.dbs))

    def __del__(self):
        global g_msg_queue, g_realTS

        if g_realTS and self._conf.mode in ['incr', 'smart']:
            logging.info('update ts before exit')
            updateOptTime(self._conf.opt_file, self._src_conn.getRepl(), g_realTS)

    def _indexParse(self, index_key):
        ''' 索引的解析，返回一个 list
        :param index_key:
        :return:
        '''
        index_list = []
        for fieldname, direction in index_key.items():
            if isinstance(direction, float):
                direction = int(direction)
            index_list.append((fieldname, direction))
        return index_list

    def _ensureIndex(self, dbname, collname):
        ''' 建立索引
        :param dbname:
        :param collname:
        :return:
        '''
        for doc in self._src_mc[dbname]['system.indexes'].find():
            if doc['ns'].replace(dbname, '', 1)[1:] != collname:
                continue

            if 'expireAfterSeconds' in doc:
                self._dest_mc[dbname][collname].create_index(self._indexParse(doc['key']),
                                                             name=doc.get('name'),
                                                             unique=doc.get('unique', False),
                                                             dropDups=doc.get('dropDups', False),
                                                             background=doc.get('background', False),
                                                             expireAfterSeconds=doc.get('expireAfterSeconds'))
            else:
                self._dest_mc[dbname][collname].create_index(self._indexParse(doc['key']),
                                                             name=doc.get('name'),
                                                             unique=doc.get('unique', False),
                                                             dropDups=doc.get('dropDups', False),
                                                             background=doc.get('background', False))

    def _ensureCollection(self, dbname, collname, copy_data=False):
        ''' 保证这个集合是 OK 的 '''
        coll_count = self._dest_mc[dbname][collname].count()

        # if have exist data and not need copy, then return
        if coll_count > 0 and not copy_data:
            logging.debug("ensure collection, %s.%s, copy data: %s" % (dbname, collname, copy_data))
            return

        logging.info(
            "ensure collection, %s.%s, copy data: %s" % (dbname, collname, copy_data))

        if coll_count == 0:
            self._ensureIndex(dbname, collname)

        if copy_data:
            cursor = self._src_mc[dbname][collname].find(spec=None, snapshot=True, timeout=False)

            count = 0
            for doc in cursor:
                count += 1
                self._dest_mc[dbname][collname].update({'_id': doc['_id']}, doc, upsert=True)

                if count % 1000 == 0:
                    logging.info('process sync dbname: %s. collname: %s, count: %d' % (
                        dbname, collname, count))

            logging.info('finish sync dbname: %s. collname: %s, count: %d' % (
                dbname, collname, count))
        else:
            logging.debug("needn't copy data")

    def _ensureDB(self, dbname, copy_data):
        # 获取源的 db，以及 collection
        index = dbname.find('.')
        if index > 0:
            dn = dbname[0:index]
            collname = dbname[index + 1:]

            if not collname.startswith('system'):
                self._ensureCollection(dn, collname, copy_data)
        else:
            for collname in self._src_mc[dbname].collection_names():
                if not collname.startswith('system'):
                    self._ensureCollection(dbname, collname, copy_data)

    def _syncOplogImpl(self):
        global g_msg_queue, g_realTS
        '''
        :param opt_time: 上次的更新时间
        :return:
        '''
        logging.info(
            "sync begin, src(%s) -> dest(%s), opt time: %s" % (self._src_conn, self._dest_conn, str(self._ts)))

        # reconnect if failed
        if not self._src_mc.alive:
            logging.error("src connect not alive, reconnect")
            self._src_mc = self._src_conn.getConn()

        count = 0
        allcount = 0
        last_up = time.time()

        while True:
            logging.info("begin optlog find")

            cursor = self._src_mc['local']['oplog.rs'].find({'ts': {'$gt': self._ts}}, tailable=True)

            if not cursor:
                logging.debug(
                    '%s has not data to sync. ' % (self._src_conn))
                time.sleep(10)
                continue

            logging.info("optlog find: %d, ts: %s" % (cursor.count(), self._ts))

            # sync
            while cursor.alive:
                try:
                    oplog = cursor.next()

                    # parse oplog
                    self._ts = oplog.get('ts')
                    ns = oplog.get('ns')

                    if not ns:
                        logging.debug("oplog is: %s" % (oplog))
                        continue

                    # 仅仅处理了 db 和 coll
                    index = ns.find('.')
                    if index > 0:
                        db = ns[0: index]
                        coll = ns[index + 1:]

                        if ns in self._conf.dbs or db in self._conf.dbs:
                            # if queue is full, sleep for a will
                            while g_msg_queue.full():
                                time.sleep(5)
                            g_msg_queue.put((db, coll, oplog), block=False)

                            num = self._syncData.get(db, 0)
                            self._syncData[db] = num + 1

                            # self._syncDst(db, coll, oplog)

                    # record the oplog information
                    allcount += 1
                    count += 1
                    if count >= self._conf.record_interval or (
                                    time.time() - last_up >= self._conf.record_time_interval):
                        last_up = time.time()
                        count = 0
                        updateOptTime(self._conf.opt_file, self._src_conn.getRepl(), g_realTS)
                        logging.info(
                            'have read: %d records, maybe not sync, read info detail: %s' % (allcount, self._syncData))
                except StopIteration, e:
                    logging.debug('%s StopIteration Exception.' % (
                        self._src_conn))

                    time.sleep(5)
                    if not cursor.alive:
                        break
                except:
                    logging.error('%s cursor error: %s' % (
                        self._src_conn, traceback.format_exc()))
                    break

            # close the cursor
            try:
                cursor.close()
            except:
                logging.error('%s cursor close error: %s' % (
                    self._src_conn, traceback.format_exc()))

    def _getSrcOptime(self):
        """ Get current optime of source mongod.  """
        rs_status = self._src_mc.admin.command({'replSetGetStatus': 1})
        members = rs_status.get('members')
        if members:
            for member in members:
                role = member.get('stateStr')
                if role == 'PRIMARY':
                    ts = member.get('optime')
                    logging.info("get primary optime")
                    return ts

        logging.error("can't get primary optime")
        return Timestamp((int)(time.time()), 0)

    def run(self):
        global g_msg_queue, g_realTS
        """ Apply oplog, 如果是全量模式，会进行集合的创建，数据的拷贝工作，然后退出；增量模式则会一直同步下去 """
        # make sure exist the opt file
        if not os.path.isfile(self._conf.opt_file):
            f = open(self._conf.opt_file, 'w')
            f.close()

        if len(self._conf.dbs) == 0:
            logging.error("dbs is empty")
            return

        for db in self._conf.dbs:
            if self._conf.mode in ['all', 'smart']:
                updateOptTime(self._conf.opt_file, self._src_conn.getRepl(), self._getSrcOptime())
                self._ensureDB(db, copy_data=True)
            else:
                self._ensureDB(db, copy_data=False)

        # 数据同步
        if self._conf.mode in ['incr', 'smart']:
            tmp_ts = getFileOptTime(self._conf.opt_file, self._src_conn.getRepl())
            self._ts = Timestamp(tmp_ts[0], tmp_ts[1])
            g_realTS = self._ts

            while True:
                try:
                    self._syncOplogImpl()
                except:
                    logging.error(
                        "sync catch an exception: %s" % (traceback.format_exc()))

                logging.error("return from sync, sleep for a will and try again")
                time.sleep(10)

        # wait for queue release and thread release
        g_msg_queue.join()


class ConsumerSynchronizer(threading.Thread):
    def __init__(self, dest_conn, conf):
        """ Constructor.
        """
        threading.Thread.__init__(self)

        self._conf = conf
        self._dest_conn = dest_conn
        self._dest_mc = self._dest_conn.getConn()

        logging.info("consumer sync dest(%s)" % (dest_conn))

    def _syncDst(self):
        global g_msg_queue, g_realTS
        '''
        :param dbname:
        :param collname:
        :param oplog:
        :return:
        '''
        count = 0
        while True:
            try:
                dbname, collname, oplog = g_msg_queue.get(timeout=5)

                op = oplog['op']

                tt = oplog.get('ts')
                if tt < g_realTS:
                    g_realTS = tt

                logging.debug('dbname: %s, collname: %s, op: %s' % (dbname, collname, op))

                if op == 'i':  # insert
                    self._dest_mc[dbname][collname].save(oplog['o'])  # if exist,recover it
                elif op == 'u':  # update
                    self._dest_mc[dbname][collname].update(oplog['o2'], oplog['o'])
                elif op == 'd':  # delete
                    self._dest_mc[dbname][collname].remove(oplog['o'])
                elif op == 'c':  # command
                    self._dest_mc[dbname].command(oplog['o'])
                elif op == 'n':  # no-op
                    logging.info('no-op')
                else:
                    logging.error('unknown command: %s' % (oplog))

                # record the oplog information
                count += 1
                if count % self._conf.record_interval == 0:
                    logging.info('have sync: %d records' % (count))
            except Queue.Empty, e:
                logging.debug('queue is empty, not sync')
            except Exception, e:
                logging.error('sync dst mongo failed: %s' % (traceback.format_exc()))
                # reconnect if failed
                while not self._dest_mc or not self._dest_mc.alive:
                    time.sleep(5)
                    logging.error(
                        "dest connect not alive, reconnect and try to insert again.")
                    # reconnect again
                    try:
                        self._dest_mc = self._dest_conn.getConn()
                    except:
                        logging.error(
                            "get dest connect exception: %s" % (traceback.format_exc()))

    def run(self):
        logging.info("start a consumer...")

        self._syncDst()
