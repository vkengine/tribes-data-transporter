from src.main.python.connection.connection import Connection
from gremlin_python.driver import client


class Client:

    @staticmethod
    def get_gremlin_conn():
        conn = Connection()

        gremlin_conn = client.Client(url=conn.get_url(),
                                     traversal_source=conn.get_traversal_source(),
                                     username=conn.get_user_name(),
                                     password=conn.get_password(),
                                     message_serializer=conn.get_message_serializer())

        return gremlin_conn
