__author__ = 'lsamaha'

import unittest2 as unittest
from uuid import uuid4
from dervish.store import Store, StoreIdException


class StoreTest(unittest.TestCase):

    def test_batch_write(self):
        """
        Show how event data interacts with remote storage via the store object using automated batch sizes.
        """
        s3bucket = 'mybucket'
        s3path = 'path/path'
        store = Store(
            MockS3Connection(), s3bucket, s3path, max_batch_size = 2, max_batch_bytes =  -1, max_push_interval = -1)
        s3 = MockS3(s3path)
        put_tracker = MockPut(s3bucket=s3bucket, s3path=s3path)
        store.get_bucket = lambda x : s3
        store.write = put_tracker.write
        guid = uuid4()
        s3.set_uuid(guid)
        data = '{"env": "dev", "event_class": "start", "event_type": "service", "product": "mimi", "subtype": "whirl"}'
        store.put(guid, data)
        self.assertEquals(0, put_tracker.count)
        self.assertEquals(None, put_tracker.last_data)
        key = store.put(guid, data)
        self.assertIsNotNone(key)
        self.assertEquals(1, put_tracker.count)
        self.assertEquals(data + '\n' + data, put_tracker.last_data)
        self.assertTrue(key.endswith(str(guid)))

    def test_timed_write(self):
        """
        Show how event data interacts with remote storage via the store object when a time threshold has been reached.
        """
        s3bucket = 'mybucket'
        s3path = 'path/path'
        max_push_interval = 60
        store = Store(
            MockS3Connection(), s3bucket, s3path, max_batch_size = -1, max_batch_bytes =  -1,
            max_push_interval = max_push_interval)
        s3 = MockS3(s3path)
        put_tracker = MockPut(s3bucket=s3bucket, s3path=s3path)
        store.get_bucket = lambda x : s3
        store.write = put_tracker.write
        now = 0
        store.get_now = lambda : now
        guid = uuid4()
        s3.set_uuid(guid)
        data = '{"env": "dev", "event_class": "start", "event_type": "service", "product": "mimi", "subtype": "whirl"}'
        store.put(guid, data)
        self.assertEquals(0, put_tracker.count)
        self.assertEquals(None, put_tracker.last_data)
        store.put(guid, data)
        self.assertEquals(0, put_tracker.count)
        self.assertEquals(None, put_tracker.last_data)
        now = now + max_push_interval
        key = store.put(guid, data)
        self.assertIsNotNone(key)
        self.assertEquals(1, put_tracker.count)
        all_data = str(data + '\n' + data + '\n' + data)
        self.assertEquals(all_data, all_data)
        self.assertEquals(all_data, str(put_tracker.last_data))
        self.assertTrue(key.endswith(str(guid)))

    def test_validate(self):
        """
        Show how IDs that wouldn't work well in S3 are rejected.
        """
        s3bucket = 'mybucket'
        s3path = 'bla/bla2'
        store = Store(
            MockS3Connection(), s3bucket, s3path, max_batch_size = -1, max_batch_bytes =  -1, max_push_interval = -1)
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
        s3_bucket = 'mybucket'
        s3_path = 'mypath'
        uuid = uuid4()
        store = Store(
            MockS3Connection(), s3_bucket, s3_path, max_batch_size = -1, max_batch_bytes = -1, max_push_interval = -1)
        s3_file = store.get_s3_path(s3_path, uuid)
        self.assertEquals("%s/%s" % (s3_path, uuid), s3_file)

class MockS3(object):

    count = 0
    last_data = None
    key = None
    last_id = None
    path = None

    def __init__(self, path):
        self.path = path

    def set_uuid(self, uuid):
        self.last_id = uuid

    def set_contents_from_string(self, data):
        self.last_data = data
        self.count += 1
        return Store(MockS3Connection(), s3bucket='mybucket', s3path='my/path').get_s3_path(self.path, self.last_id)

class MockS3Connection(object):
    pass

class MockS3Key(object):
    def __init__(self, key):
        self.key = key

class MockPut(object):

    count = 0
    last_data = None
    s3bucket = None
    s3path = None

    def __init__(self, s3bucket, s3path):
        self.s3bucket = s3bucket
        self.s3path = s3path

    def write(self, data):
        self.count += 1
        self.last_data = data

if __name__ == '__main__':
    unittest.main()
