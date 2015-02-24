__author__ = 'luke'

from boto.dynamodb2.table import Table, Item, HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
from boto.exception import JSONResponseError
from uuid import uuid4
import json, time, logging

class Index(object):

    conn = None
    table = None
    pause = 2000
    read_through = 1
    write_through = 1
    at_read_through = 1
    at_write_through = 1
    hash_key = 'event_uuid'
    at = 'at'

    def __init__(self, conn, table_name='event', create_timeout=360, logger=logging.getLogger('Service.Python.Logger')):
        self.conn = conn
        self.logger = logger
        if not self.exists(table_name):
            self.create_table(table_name, create_timeout, self.pause)
        self.table = Index.table_for_name(table_name)

    def put(self, data_str):
        self.logger.info("putting item")
        data = json.loads(data_str)
        if not self.hash_key in data:
            data[self.hash_key] = str(uuid4())
        if not self.at in data:
            data[self.at] = int(time.time() * 1000)
        item = self.create_item(data)
        item.save()
        self.logger.info("saved item %s " % (data[self.hash_key]))
        return data[self.hash_key]

    def create_item(self, data):
        return Item(self.table, data=data)

    def exists(self, table_name):
        desc = None
        try:
            desc = self.conn.describe_table(table_name=table_name)
            self.logger.info("found table %s" % table_name)
        except JSONResponseError, e:
            if 'Table' in e.message and 'not found' in e.message:
                pass
            else:
                raise e
        return desc is not None

    def describe(self, table_name):
        return self.conn.describe_table(table_name)

    @staticmethod
    def table_for_name(table_name):
        return Table(table_name)

    def create_table(self, table, create_timeout, pause):
        self.logger.info("creating table %s" % table)
        self.table_name = Table.create(table, schema=[
            HashKey(self.hash_key),
            RangeKey('at', data_type=NUMBER)], throughput={
            'read':self.at_read_through,
            'write': self.at_write_through,
            },
                                  global_indexes=[
                                      GlobalAllIndex('productat', parts=[
                                          HashKey('product'),
                                          RangeKey('at'),
                                          ],
                                                     throughput={
                                                         'read':1,
                                                         'write':1
                                                     })
                                  ]
        )
        created = False
        now = time.time()
        while not created:
            created = self.exists(table)
            if not created:
                time.sleep(pause)
                self.logger.info("waiting %d seconds" % create_timeout)
                if create_timeout < 0 or time.time() < now + create_timeout:
                    raise StoreCreateTimeoutException("unable to create %s after %d seconds" % (table, create_timeout))
        print

class StoreCreateTimeoutException(Exception):
    pass

