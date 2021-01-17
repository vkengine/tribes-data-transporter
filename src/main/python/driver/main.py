from src.main.python.connection.client import Client
from src.main.python.utils.utils import Utils


if __name__ == '__main__':
    gremlin_conn = Client().get_gremlin_conn()

    Utils().cleanup_graph(gremlin_conn)

    Utils().read_files_and_create_vertex(gremlin_conn)

    # insert_vertices(gremlin_conn)
    #
    # insert_edges(gremlin_conn)
