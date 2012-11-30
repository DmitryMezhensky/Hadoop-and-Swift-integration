# Copyright (c) 2011 OpenStack, LLC.
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

try:
    import simplejson as json
except ImportError:
    import json

from webob import Request
from webob.exc import HTTPMethodNotAllowed

from swift.common.ring import Ring
from swift.common.utils import get_logger, split_path


class ObjectEndpoint(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='object_endpoint')
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.object_ring = Ring(swift_dir, ring_name='object')

    def __call__(self, env, start_response):
        request = Request(env)

        url_prefix = '/object_endpoint/'

        if request.path.startswith(url_prefix):

            if request.method != 'GET':
                raise HTTPMethodNotAllowed()

            aco = split_path(request.path[len(url_prefix) - 1:], 1, 3, True)
            account = aco[0]
            container = aco[1]
            obj = aco[2]
            if obj.endswith('/'):
                obj = obj[:-1]

            object_partition, objects = self.object_ring.get_nodes(
                account, container, obj)

            endpoint_template = 'http://{ip}:{port}/{device}/{partition}/' + \
                                '{account}/{container}/{obj}'
            endpoints = []
            for element in objects:
                endpoint = endpoint_template.format(ip=element['ip'],
                                                    port=element['port'],
                                                    device=element['device'],
                                                    partition=object_partition,
                                                    account=account,
                                                    container=container,
                                                    obj=obj)
                endpoints.append(endpoint)

            start_response('200 OK', {})
            return json.dumps(endpoints)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def object_endpoint_filter(app):
        return ObjectEndpoint(app, conf)
    return object_endpoint_filter
