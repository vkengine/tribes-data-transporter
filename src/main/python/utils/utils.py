import json
import time


class Utils:
    _gremlin_cleanup_graph = "g.V().drop()"
    _gremlin_add_vertex = "g.addV('id','{IdUnique}').{properties_formulated}"
    _gremlin_add_property = "property('{property_name}','{property_value}')"

    _gremlin_add_edge = "g.V().has('label',TextP.startingWith('{FromLabel}')).has('IdObject','{FromIdObject}')" \
                        ".addE('{IdUnique}')" \
                        ".to(g.V().has('label',TextP.startingWith('{ToLabel}')).has('IdObject','{ToIdObject}'))" \
                        ".{properties_formulated}"

    @staticmethod
    def add_vertex_and_edge(client, query):
        time.sleep(1)
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tAdded:\n\t{0}\n".format(
                callback.result().one()))
        else:
            print("Something went wrong with this query: {0}".format(query))

    @staticmethod
    def generate_add_edge_query(row):
        properties_unformulated = row['Property']
        list_of_properties = []
        for key, value in properties_unformulated.items():
            list_of_properties.append(Utils._gremlin_add_property.format(property_name=key, property_value=value))
        properties = ".".join(list_of_properties)
        query = Utils._gremlin_add_edge.format(FromLabel=row['FromLabel'],
                                               FromIdObject=row['FromIdObject'],
                                               IdUnique=row['IdUnique'],
                                               ToLabel=row['ToLabel'],
                                               ToIdObject=row['ToIdObject'],
                                               properties_formulated=properties)
        return query

    @staticmethod
    def generate_add_vertex_query(row):
        label = '::'.join(row['Label'])
        properties_unformulated = row['Property']
        list_of_properties = [Utils._gremlin_add_property.format(property_name='label', property_value=label)]
        for key, value in properties_unformulated.items():
            list_of_properties.append(Utils._gremlin_add_property.format(property_name=key, property_value=value))
        # TODO :: partition key adding property tribes tribes remove this
        list_of_properties.append("property('tribes','tribes')")
        properties = ".".join(list_of_properties)
        query = Utils._gremlin_add_vertex.format(IdUnique=row['IdUnique'], properties_formulated=properties)
        return query

    @staticmethod
    def check_egde_exits(client, unique_id):
        query = "g.E().has('label','{IdUnique}')".format(IdUnique=unique_id)
        callback = client.submitAsync(query)
        if str(callback.result().one()) == "[]":
            return False
        else:
            return True

    @staticmethod
    def cleanup_graph(client):
        callback = client.submitAsync(Utils._gremlin_cleanup_graph)
        if callback.result() is not None:
            print("Cleaned up the graph")
        print("\n")

    @staticmethod
    def read_files_and_create_vertex(gremlin_conn):
        files = ['file-1.json', 'file-2.json', 'file-3.json']
        for file in files:
            file_name = '/Users/vanketeshkumar/Projects/tribes-data-transporter/resources/{file_name}' \
                .format(file_name=file)
            records = json.load(open(file_name))
            for row in records:
                if row['Kind'] == 'node':
                    query = Utils.generate_add_vertex_query(row)
                    Utils.add_vertex_and_edge(gremlin_conn, query)

    @staticmethod
    def read_files_and_create_edges(gremlin_conn):
        files = ['file-1.json', 'file-2.json', 'file-3.json']
        for file in files:
            file_name = '/Users/vanketeshkumar/Projects/tribes-data-transporter/resources/{file_name}' \
                .format(file_name=file)
            records = json.load(open(file_name))
            for row in records:
                if row['Kind'] == 'relationship':
                    exists = Utils.check_egde_exits(gremlin_conn, row['IdUnique'])
                    query = Utils.generate_add_edge_query(row)
                    if exists and row['DeDuplication']:
                        continue
                    else:
                        Utils.add_vertex_and_edge(gremlin_conn, query)
