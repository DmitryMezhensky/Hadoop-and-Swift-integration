# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from ConfigParser import ConfigParser, NoSectionError, NoOptionError

from swift.common.memcached import MemcacheRing


class MemcacheMiddleware(object):
    """
    Caching middleware that manages caching in swift.
    """

    def __init__(self, app, conf):
        self.app = app
        self.memcache_servers = conf.get('memcache_servers')
        serialization_format = conf.get('memcache_serialization_support')

        if not self.memcache_servers or serialization_format is None:
            path = os.path.join(conf.get('swift_dir', '/etc/swift'),
                                'memcache.conf')
            memcache_conf = ConfigParser()
            if memcache_conf.read(path):
                if not self.memcache_servers:
                    try:
                        self.memcache_servers = \
                            memcache_conf.get('memcache', 'memcache_servers')
                    except (NoSectionError, NoOptionError):
                        pass
                if serialization_format is None:
                    try:
                        serialization_format = \
                            memcache_conf.get('memcache',
                                              'memcache_serialization_support')
                    except (NoSectionError, NoOptionError):
                        pass

        if not self.memcache_servers:
            self.memcache_servers = '127.0.0.1:11211'
        if serialization_format is None:
            serialization_format = 2
        else:
            serialization_format = int(serialization_format)

        self.memcache = MemcacheRing(
            [s.strip() for s in self.memcache_servers.split(',') if s.strip()],
            allow_pickle=(serialization_format == 0),
            allow_unpickle=(serialization_format <= 1))

    def __call__(self, env, start_response):
        env['swift.cache'] = self.memcache
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def cache_filter(app):
        return MemcacheMiddleware(app, conf)

    return cache_filter
