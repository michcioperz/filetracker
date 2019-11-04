from datetime import datetime
from math import ceil
from time import mktime

import minio
import pytz
from six.moves.urllib_parse import urlparse

from filetracker.client.data_store import DataStore
from filetracker.utils import split_name, versioned_name

class _FileLikeFromResponse(object):
    def __init__(self, response):
        self.iter = response.stream(16*1024)
        self.data = b''

    def read(self, size=None):
        if size is None:
            # read all remaining data
            return self.data + b''.join(c for c in self.iter)
        else:
            while len(self.data) < size:
                try:
                    self.data += next(self.iter)
                except StopIteration:
                    break
            result, self.data = self.data[:size], self.data[size:]
            return result

class S3DataStore(DataStore):
    def __init__(self, url):
        urlparts = urlparse(url)
        secure = urlparts.scheme == 'https'
        port = ':{}'.format(urlparts.port) if urlparts.port else ''
        self.bucket = urlparts.path.lstrip('/')
        self.client = minio.Minio(urlparts.hostname + port,
                secure=secure,
                access_key=urlparts.username,
                secret_key=urlparts.password)
        assert self.client.bucket_exists(self.bucket)

    def add_file(self, name, filename, compress_hint=True):
        name = name.lstrip('/')
        etag = self.client.fput_object(self.bucket, name, filename)
        stat = self.client.stat_object(self.bucket, name)
        assert stat.etag == etag
        return '/'+versioned_name(name, int(ceil(mktime(stat.last_modified))))

    def exists(self, name):
        name, version = split_name(name)
        name = name.lstrip('/')
        stat = self.client.stat_object(self.bucket, name)
        return version is None or int(ceil(mktime(stat.last_modified))) == version

    def file_version(self, name):
        name, version = split_name(name)
        name = name.lstrip('/')
        return int(ceil(mktime(self.client.stat_object(self.bucket, name).last_modified)))

    def file_size(self, name):
        name, version = split_name(name)
        name = name.lstrip('/')
        stat = self.client.stat_object(self.bucket, name)
        assert version is None or int(ceil(mktime(stat.last_modified))) == version
        return stat.size

    def get_stream(self, name):
        name, version = split_name(name)
        name = name.lstrip('/')
        stat = self.client.stat_object(self.bucket, name)
        remote_version = int(ceil(mktime(stat.last_modified)))
        assert version is None or remote_version == version
        # TODO: strftime is locale-dependent
        return _FileLikeFromResponse(self.client.get_object(self.bucket, name, {
            'If-Match': stat.etag
            })), '/'+versioned_name(name, remote_version)

    def delete_file(self, name):
        name, version = split_name(name)
        name = name.lstrip('/')
        # TODO: race condition
        self.client.remove_object(self.bucket, name)
