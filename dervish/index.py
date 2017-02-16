__author__ = 'luke'

from boto.dynamodb2.table import Table, Item, HashKey, RangeKey, GlobalAllIndex
from boto.exception import JSONResponseError
from uuid import uuid4
import json, time, logging

class Index(object):

    hash_key = 'event_uuid'
    env = 'env'
    product = 'product'
    event_class = 'event_class'
    event_type = 'event_type'
    at = 'at'
    none = 'none'
    envproductclasstypeat = 'epcta'

    def __init__(self, conn, table_name='event', create_timeout=360, logger=logging.getLogger('Service.Python.Logger')):
        self.table = None
        self.pause = 2000
        self.at_read_through = 1
        self.at_write_through = 1
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
        self.ensure_key_elements(data)
        self.create_compound_key(data)
        item = self.create_item(data)
        item.save()
        self.logger.info("saved item %s " % (data[self.hash_key]))
        return data[self.hash_key]

    def create_compound_key(self, data):
        if not self.envproductclasstypeat in data:
            data[self.envproductclasstypeat] = \
                "%s|%s|%s|%s|%s" % \
                (data[self.env], data[self.product], data[self.event_class], data[self.event_type], data[self.at])

    def ensure_key_elements(self, data):
        if self.env not in data:
            data[self.env] = self.none
        if self.product not in data:
            data[self.product] = self.none
        if self.event_class not in data:
            data[self.event_class] = self.none
        if self.event_type not in data:
            data[self.event_type] = self.none

    def create_item(self, data):
        return Item(self.table, data=data)

    def exists(self, table_name):
        desc = None
        try:
            desc = self.conn.describe_table(table_name=table_name)
            self.logger.info("found table %s" % table_name)
        except JSONResponseError as e:
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
        self.table_name = Table.create(
            table, schema=[
                HashKey(self.hash_key)
            ],
            throughput={'read':self.at_read_through,'write': self.at_write_through,},
            global_indexes=[
                GlobalAllIndex(self.envproductclasstypeat, parts=[HashKey('product'), RangeKey(self.envproductclasstypeat)],
                               throughput={'read':1,'write':1})
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

class StoreCreateTimeoutException(Exception):
    pass

