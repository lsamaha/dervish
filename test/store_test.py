__author__ = 'lsamaha'

import unittest2 as unittest
from uuid import uuid4
from dervish.store import Store, StoreIdException, StoreNotFoundException


class StoreTest(unittest.TestCase):

    def test_store(self):
        """
        Show how event data interacts with remote storage via the store object.
        """
        store = Store(MockS3Connection())
        path = 'path/path'
        s3 = MockS3(path)
        put_tracker = MockPut()
        store.get_bucket = lambda x : s3
        store.put = put_tracker.put
        guid = uuid4()
        s3.set_id(guid)
        data = '{"env": "dev", "event_class": "start", "event_type": "service", "product": "mimi", "subtype": "whirl"}'
        key = store.put('s3://mybucket', path, guid, data)
        self.assertIsNotNone(key)
        self.assertEquals(1, put_tracker.count)
        self.assertEquals(guid, put_tracker.last_id)
        self.assertEquals(data, put_tracker.last_data)
        self.assertTrue(key.endswith(str(guid)))

    def test_validate(self):
        """
        Show how IDs that wouldn't work well in S3 are rejected.
        """
        store = Store(MockS3Connection())
        try:
            store.validate(None)
            self.assertFalse('Using validate on an empty key should result in an exception', True)
        except StoreIdException:
                pass
        try:
            id_with_slash = 'my/id'
            store.validate(id_with_slash)
            self.assertFalse('Using validate on a key with a slash should result in an exception', True)
        except StoreIdException:
                pass

    def test_key(self):
        """
        The S3 key is a path. Demonstrate here how it is formed from the provided path and the ID.
        """
        s3_path = 'mypath'
        id = uuid4()
        self.assertEquals("%s/%s" % (s3_path, id), Store(MockS3Connection()).get_s3_path(s3_path, id))

class MockS3(object):

    count = 0
    last_data = None
    key = None
    last_id = None
    path = None

    def __init__(self, path):
        self.path = path

    def set_id(self, id):
        self.last_id = id

    def set_contents_from_string(self, data):
        self.last_data = data
        self.count += 1
        return Store(MockS3Connection()).get_s3_path(self.path, self.last_id)

class MockS3Connection(object):
    pass

class MockS3Key(object):
    def __init__(self, key):
        self.key = key

class MockPut(object):

    count = 0
    last_id = None
    last_data = None

    def put(self, s3bucket, path, id, data):
        self.count += 1
        self.last_id = id
        self.last_data = data
        return Store(None).get_s3_path(path, id)

if __name__ == '__main__':
    unittest.main()
