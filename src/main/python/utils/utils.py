import json
import time


class Utils:
    _gremlin_cleanup_graph = "g.V().drop()"
    _gremlin_add_vertex = "g.addV('id','{IdUnique}').{properties_formulated}"
    _gremlin_add_property = "property('{property_name}','{property_value}')"

    @staticmethod
    def add_vertex(client, query):
        print("\tRunning this Gremlin query:\n\t{0}\n".format(query))
        time.sleep(2)
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this vertex:\n\t{0}\n".format(
                callback.result().one()))
        else:
            print("Something went wrong with this query: {0}".format(query))



    @staticmethod
    def generate_add_edge_query(row):
        pass

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
    def execute_query():
        pass

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
                    Utils.add_vertex(gremlin_conn, query)
