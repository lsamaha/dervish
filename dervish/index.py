__author__ = 'luke'

from boto.dynamodb2.table import Table, Item, HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
from boto.exception import JSONResponseError
import time

class Index(object):

    conn = None
    table = None
    pause = 2000
    read_through = 1
    write_through = 1
    at_read_through = 1
    at_write_through = 1

    def __init__(self, conn, table = 'event', create_timeout=360):
        self.conn = conn
        if not self.exists(table):
            self.create_table(table, create_timeout, self.pause)
        self.table = Index.table_for_name(self, table)

    def put(self, data):
        item = self.create_item(data)
        item.save()

    def create_item(self, data):
        return Item(self.table, data=data)

    def exists(self, table_name):
        desc = None
        try:
            desc = self.conn.describe_table(table_name=table_name)
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
        self.table = Table.create(table, schema=[
            HashKey('message_uid'),
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
        if create_timeout > 0:
            print "waiting %d seconds" % create_timeout,
        while not created:
            created = self.exists(table)
            time.sleep(pause)
            print '.',
            if create_timeout < 0 or time.time() < now + create_timeout:
                print
                raise StoreCreateTimeoutException("unable to create %s after %d seconds" % (table, create_timeout))
        print

class StoreCreateTimeoutException(Exception):
    pass

