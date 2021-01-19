__author__ = 'v-kumar (v.kmr@yahoo.com)'

from gremlin_python.driver import serializer
import os

"""
please set URL, USERNAME and PASSWORD in system before execution
"""


class Connection:

    def __init__(self):
        self._url = os.environ['URL']
        self._traversal_source = 'g'
        self._user_name = os.environ['USERNAME']
        self._password = os.environ['PASSWORD']
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
