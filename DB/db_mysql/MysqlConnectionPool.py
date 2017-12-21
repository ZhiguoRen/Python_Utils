#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @Author  : ren@uwa4d.com

import Queue
#import pymysql
from DBController import DBController

class DBControllerPool():
    """
    Usage:
        conn_pool = ConnectionPool(config)

        db = conn_pool.get_connection()
        db.query('SELECT 1', [])
        conn_pool.return_connection(db)

        conn_pool.close()
    """
    #def __init__(self, conf, max_pool_size=20):
    #    self.conf = conf
    #    self.max_pool_size = max_pool_size
    #    self.initialize_pool()

    def __init__(self,host,port,db_name,db_user_name,db_psw,max_pool_size=20,conn_at_start=True):
        self.conn_at_start=conn_at_start
        self.max_pool_size = max_pool_size
        self.current_conn_size=0
        self.conf={}
        self.conf["host"]=host
        self.conf["port"]=port
        self.conf["db_name"]=db_name
        self.conf["db_user_name"]=db_user_name
        self.conf["db_psw"]=db_psw
        self.initialize_pool()

    def initialize_pool(self):
        #todo:需要另一队列来监控正在使用的connection
        self.pool = Queue.Queue(maxsize=self.max_pool_size)
        if self.conn_at_start:
            for _ in range(0, self.max_pool_size):
                self.pool.put_nowait(
                    DBController(   host=self.conf["host"],
                                    db_name=self.conf["db_name"],
                                    db_user_name =self.conf["db_user_name"],
                                    psd=self.conf["db_psw"],
                                    port=self.conf["port"]
                                    )
                )
                self.current_conn_size+=1

    def get_dbc(self):
        # returns a db instance when one is available else waits until one is
        if not self.conn_at_start and self.current_conn_size<self.max_pool_size:
            dbc=DBController(       host=self.conf["host"],
                                    db_name=self.conf["db_name"],
                                    db_user_name =self.conf["db_user_name"],
                                    psd=self.conf["db_psw"],
                                    port=self.conf["port"]
                                    )
            if not dbc:
                print "cannot generate dbccontroller"
                return None
            self.current_conn_size+=1
            dbc.new_cur()
            return dbc

        dbc = self.pool.get(True)
        dbc.new_cur()
        return dbc

    def return_dbc(self, dbc):
        dbc.close_cur()
        try:
            self.pool.put_nowait(dbc)
            dbc = None
        except Exception,e:
            print e
        return dbc

    def close(self):
        while not self.is_empty():
            self.pool.get().close()
        self.current_conn_size=self.pool.qsize()

    #def ping(self, db):
    #    data = db.query('SELECT 1', [])
    #    return data

    #def get_initialized_connection_pool(self):
    #    return self.pool

    def is_empty(self):
        return self.pool.empty()