__author__ = 'luke'

from boto.s3.connection import S3Connection
from boto.s3.key import Key

class Store(object):

    s3conn = None

    def __init__(self, s3conn):
        self.s3conn = s3conn

    def put(self, s3bucket, path, id, data):
        """
        Write event data to S3.
        """
        if s3bucket and path:
            path = path if path.endswith("/") else "%s/" % path
        else:
            raise StoreNotFoundException(
                "The s3bucket and path arguments must not be null but were %s%s " % (s3bucket, path))
        key = Key(self.s3conn.get_bucket(s3bucket))
        key.key = self.get_s3_path(path, id)
        key.set_contents_from_string(data)
        return key.key

    def get_bucket(self, s3conn):
        return Key(s3conn)

    def get_s3_path(self, path, id):
        self.validate(id)
        return "%s%s" % (path if path.endswith('/') else "%s/" % path, str(id))

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



