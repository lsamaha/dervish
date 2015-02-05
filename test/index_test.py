__author__ = 'lsamaha'

import unittest2 as unittest
from dervish.index import Index


class IndexTest(unittest.TestCase):

    def test_index_put(self):
        data = '{"message_uid":"ae584408-5118-4149-b30a-fdedc32aa4fe","at":1423164438.46,' \
               '"env": "dev", "event_class": "start", "event_type": "whirl", "product": "mimi", "subtype": "forth"}'
        table = MockTable()
        Index.table_for_name = lambda x, y : table
        index = Index(conn = MockConn(), table='mocked')
        item = MockItem(data)
        index.create_item = lambda x : item
        index.put(data)
        self.assertEquals(1, item.count)
        self.assertEquals(data, item.last_put)

class MockTable(object):
    pass

class MockItem(object):
    count = 0
    last_put = None

    def __init__(self, data):
        self.data = data

    def save(self):
        self.put(self.data)

    def put(self, data):
        self.last_put = data
        self.count += 1

class MockConn(object):
    def describe_table(self, table_name):
        return {}
