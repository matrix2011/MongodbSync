#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pymongo import MongoClient
import traceback
import ConfigParser
import pymongo
from pymongo import ReplicaSetConnection

class SyncConfig(object):
    ''' 配置文件抽象，是对本项目配置文件的一个抽象表达，为了便于后续的使用 '''

    def __init__(self, conf):
        self._conf = conf

        # sync-info
        self.mode = None
        self.record_interval = None
        self.record_time_interval = None
        self.opt_file = None
        self.log_file = None
        self.all_dbs = None
        self.dbs = None
        self.queue_num = None
        self.threads = None

        # mongo-src
        self.mongo_src = None

        # mongo-dest
        self.mongo_dest = None

    def parse(self):
        try:
            config = ConfigParser.RawConfigParser()
            config.read(self._conf)

            # sync-info
            self.mode = config.get("sync-info", "mode")
            self.opt_file = config.get("sync-info", "opt_file")

            if not self.opt_file:
                return False

            if self.mode not in ['all', 'incr', 'smart']:
                logging.error("mode is not valid: %s" % (self.mode))
                return False

            # read interval
            self.record_interval = config.getint("sync-info", "record_interval")
            self.record_time_interval = config.getint("sync-info", "record_time_interval")

            self.all_dbs = config.getboolean("sync-info", "all_dbs")
            if not self.all_dbs:
                self.dbs = config.get("sync-info", "dbs").split(",")

            # queue num and threads num
            self.queue_num = config.getint("sync-info", "queue_num")
            self.threads = config.getint("sync-info", "threads")

            logging.info("mode: %s, record_interval: %d, record_time_interval: %d, opt file: %s, all dbs: %s, sync dbs: %s, queue num: %d, threads: %d" % (
                self.mode, self.record_interval, self.record_time_interval, self.opt_file, self.all_dbs, self.dbs, self.queue_num, self.threads))

            # mongo-src
            self.mongo_src = (
                config.get("mongo-src", "addr"), config.get("mongo-src", "user"), config.get("mongo-src", "pwd"))
            logging.info("mongo src: %s" % str(self.mongo_src))

            if not self.mongo_src[0]:
                return False

            # mongo-dest
            self.mongo_dest = (
                config.get("mongo-dest", "addr"), config.get("mongo-dest", "user"), config.get("mongo-dest", "pwd"))
            logging.info("mongo dest: %s" % str(self.mongo_dest))

            if not self.mongo_dest[0]:
                return False
        except:
            logging.error("parse file exception: %s" % (traceback.format_exc()))
            return False

        return True

    def __str__(self):
        return "mode: %s, record_interval: %d, record_time_interval: %d, opt_file: %s, all_dbs: %s, dbs: %s, queue num: %d, threads: %d, mongo_src: %s, mongo_dest: %s" % (
            self.mode, self.record_interval, self.record_time_interval, self.opt_file, self.all_dbs, self.dbs, self.queue_num, self.threads, self.mongo_src, self.mongo_dest)


class MongoConnInfo(object):
    def __init__(self, addr, user=None, pwd=None, repl=None):
        '''
        :param addrs: 地址信息，为一个数组，形如 ["ip:port", "ip:port"]
        :param user: 账号
        :param pwd: 密码
        :param repl: 副本集合的名称
        :return:
        '''
        self._addr = addr
        self._user = user
        self._pwd = pwd
        self._repl = repl

        logging.info("addrs: %s, user: %s, pwd: %s, repl: %s" % (self._addr, self._user, self._pwd, self._repl))

    def getAddr(self):
        return self._addr

    def getUser(self):
        return self._user

    def getPwd(self):
        return self._pwd

    def getRepl(self):
        return self._repl

    def getConn(self):
        ''' 获取 primary 的连接信息 '''
        if self._repl:
            client = ReplicaSetConnection(self._addr,
                                          replicaSet=self._repl,
                                          read_preference=pymongo.read_preferences.ReadPreference.PRIMARY)
        else:
            client = MongoClient(self._addr)

        if self._user and self._pwd:
            client.admin.authenticate(self._user, self._pwd)

        return client

    def __str__(self):
        return "addrs: %s, user: %s, pwd: %s, repl: %s" % (self._addr, self._user, self._pwd, self._repl)