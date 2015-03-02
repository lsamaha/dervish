__author__ = 'luke'

import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class Store(object):

    def __init__(self, s3conn, s3bucket, s3path,
                 max_batch_size = 100000, max_batch_bytes =  120 * 1024 * 1024, max_push_interval = 20 * 60):
        self.last_batch = []
        self.batch = []
        self.batch_bytes = 0
        self.batch_name = None
        self.batch_start = None
        self.s3conn = s3conn
        self.s3bucket = s3bucket
        self.s3path = s3path
        self.max_batch_size = max_batch_size
        self.max_batch_bytes = max_batch_bytes
        self.max_push_interval = max_push_interval
        self.batch = []
        if self.s3bucket and self.s3path:
            self.s3path = self.s3path if self.s3path.endswith("/") else "%s/" % self.s3path
        else:
            raise StoreNotFoundException(
                "The s3bucket and path arguments must not be null but were %s %s " % (self.s3bucket, self.s3path))

    # TODO synchronize? write in separate thread?
    def put(self, uuid, data):
        """
        Batch data. Write when full or timer expires.
        """
        s3file_written = None
        self.validate(id)
        self.batch.append(data)
        self.batch_bytes += len(data)
        if self.batch_start == None:
            self.batch_start = self.get_now()
        if self.batch_name == None:
            self.batch_name = str(uuid)
        if (self.max_batch_size > 0 and len(self.batch) >= self.max_batch_size) or \
                (self.max_batch_bytes > 0 and self.batch_bytes > self.max_batch_bytes) or \
                (self.max_push_interval > 0 and self.get_now() - self.batch_start >= self.max_push_interval):
            self.new_batch()
            self.write_batch(self.last_batch)
            s3file_written = self.get_s3_path(self.s3path, self.batch_name)
            self.last_batch = None
        return s3file_written

    def get_now(self):
        return time.time()

    def new_batch(self):
        """
        Lock in the current batch and write it. Create a new batch and reset all counters.
        """
        self.last_batch = self.batch
        self.batch = []
        self.batch_start = None
        self.batch_bytes = 0

    def write_batch(self, batch):
        """
        Write each entry in a batch of event data to S3 as a single file.
        """
        data = '\n'.join(batch)
        return self.write(data)

    def write(self, data):
        key = Key(self.s3conn.get_bucket(self.s3bucket))
        key.key = self.get_s3_path(self.s3path, self.batch_name)
        key.set_contents_from_string(data)
        return key.key

    def get_bucket(self, s3conn):
        return Key(s3conn)

    def get_s3_path(self, path, uuid):
        path = path if path.endswith('/') else "%s/" % path
        return "%s%s" % (path, str(uuid))

    def validate(self, id):
        """
        Raise exception for any IDs that cannot be directly written as part of an S3 path.
        :param id: The proposed id to be used in the S3 path as a stringifiable object.
        :raises StoreIdException If the ID is empty or contains characters that would write/read poorly as an S3 path.
        """
        if not id:
            raise(StoreIdException('An ID must be provided for the data to be stored'))
        elif '/' in str(id):
            raise(StoreIdException('A slash must not be present in the ID since it is used as part of a path'))
        pass

    def get_s3_conn(self):
        return S3Connection().get_bucket(self.s3bucket)

class StoreNotFoundException(Exception):
    pass

class StoreIdException(Exception):
    pass



