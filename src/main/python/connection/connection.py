__author__ = 'v-kumar (v.kmr@yahoo.com)'

from gremlin_python.driver import serializer


class Connection:

    def __init__(self):
        self._url = 'wss://tdt.gremlin.cosmos.azure.com:443/'
        self._traversal_source = 'g'
        self._user_name = '/dbs/tribes/colls/tribes-data-transporter'
        self._password = 'svzdwqiAcPcaX5wiz93qFm3z05QO3C7GLq0XM47YjT8gzNJuDND77LxXi7o8Y8eu8Tz4qx1HASmnKBACm98GpQ=='
        self._message_serializer = serializer.GraphSONSerializersV2d0()

    def get_url(self):
        return self._url

    def get_traversal_source(self):
        return self._traversal_source

    def get_user_name(self):
        return self._user_name

    def get_password(self):
        return self._password

    def get_message_serializer(self):
        return self._message_serializer
