import json
from py2neo import Graph
import pandas as pd
import helper_functions as hf


def create_node_creation_query(json, seq_id, node_properties, schema_properties, arr):
    '''
        Generates compound cypher query for node creation
            Parameters:
                json (dict): A dict containing json data to be converted
                seq_id (int): An integer containing the row id in graph schema
                node_properties (list): A list containing column names to be set as node property
                schema_properties (list): A list containing property names to be set on node
                arr (arr): A list containing path to the key
            Returns:
                void
    '''
    if len(arr) == 1:
        global nodes_creation_query
        nodes_creation_query += '''
        WITH null as nothing
        MERGE (node:{label} {{ {prop1}: "{id_object}" }})
            ON CREATE
                SET node.{prop2} = TIMESTAMP(),
                    node.{prop3} = "{val1}",
                    node.{prop4} = "{val2}",
                    node.{prop5} = "{val3}"
        '''.format(
                label = schema_df['Label (Primary)'][idx], 
                id_object = json[arr[0]],
                prop1 = node_properties[0],
                prop2 = node_properties[1],
                prop3 = node_properties[2],
                val1 = json[(schema_df[schema_properties[2]][seq_id].split('►'))[-1]] if type(schema_df[schema_properties[2]][seq_id]) == str else 'null',
                prop4 = node_properties[3],
                val2 = json[(schema_df[schema_properties[3]][seq_id].split('►'))[-1]] if type(schema_df[schema_properties[3]][seq_id]) == str else 'null',
                prop5 = node_properties[4],
                val3 = json[(schema_df[schema_properties[4]][seq_id].split('►'))[-1]] if type(schema_df[schema_properties[4]][seq_id]) == str else 'null',
            )
    else:
        if arr[1] != '0-N':
            create_node_creation_query(json[arr[0]], seq_id, node_properties, schema_properties, arr[1:])
        else:
            for element in json[arr[0]]:
                create_node_creation_query(element, seq_id, node_properties, schema_properties, arr[2:])


def create_relationship_creation_query(label_seqId_1, label_seqId_2, relationship):
    '''
        Generates compound cypher query for relationship creation
            Parameters:
                label_seqId_1 (int): An integer containing seqId for relationship initiater node
                label_seqId_2 (int): An integer containing seqId for node to which relationship is created
                relationship (str): A string containing name of relationship
            Returns:
                void
    '''
    global relationship_creation_query

    id_obj1 = schema_df['IdObject'][label_seqId_1 - 1]
    id_obj2 = schema_df['IdObject'][label_seqId_2 - 1]
    id_obj_arr1 = id_obj1.split('►')
    id_obj_arr2 = id_obj2.split('►')

    elements_arr_1 = hf.get_elements(json_data, id_obj_arr1)
    elements_arr_2 = hf.get_elements(json_data, id_obj_arr2)

    label1 = schema_df['Label (Primary)'][label_seqId_1 - 1]
    label2 = schema_df['Label (Primary)'][label_seqId_2 - 1]
    
    for fromNode in elements_arr_1:
        for toNode in elements_arr_2:
            if fromNode != toNode:
                relationship_creation_query += '''
                    WITH null as nothing
                    MATCH (node1:{l1} {{ id_object: '{id1}' }})
                    MATCH (node2:{l2} {{ id_object: '{id2}' }})
                    CALL apoc.merge.relationship(node1, '{rel}', null, null, node2, null)
                    YIELD rel
                    '''.format(
                        l1 = label1,
                        id1 = fromNode,
                        l2 = label2,
                        id2 = toNode,
                        rel = relationship
                    )


nodes_creation_query = ''
relationship_creation_query = ''

# JSON data which is to be converted into graph
f = open('./sample_data/game_sample_json.json', 'r')
json_data = json.load(f)

# Graph Schema containing instructions for node and relationship creation
schema_df = pd.read_csv('./sample_data/graph_schema.csv')
schema_df_types = list(schema_df['Node/Relationship'])

# Connecting to the graph database
dei_task = Graph("bolt://localhost:7687", password = "test")

# Traversing schema_df to find label to create nodes
for idx, element in enumerate(schema_df_types):
    column_names = list(schema_df.columns)
    if element == 'Node':
        id_object = schema_df['IdObject'][idx]
        id_object_list = list(id_object.split('►'))
        schema_properties = column_names[6:]
        node_properties = list(map(hf.pascal_to_snake, schema_properties))
        create_node_creation_query(json_data, idx, node_properties, schema_properties, id_object_list)
    elif element == 'Relationship':
        pair_seq_ids = schema_df[column_names[5]][idx]
        pair_seq_ids_list = list(pair_seq_ids.split('->'))
        seq_ids = [int(label_string[1:-1]) for label_string in pair_seq_ids_list]
        relationship = schema_df[column_names[2]][idx]
        create_relationship_creation_query(seq_ids[0], seq_ids[1], relationship)


# Setting NA propety value as null and "True" as true
nodes_creation_query = hf.resolve_node_prop_values(nodes_creation_query)

print('Creating Nodes...')

# Node creation query execution
nodeCreation = dei_task.run(nodes_creation_query)

# node creation query stats
nodeCreationStats = nodeCreation.stats()

# Node creation status message
isNewlyCreated = nodeCreationStats.nodes_created 
if isNewlyCreated:
    print('Nodes Created Successfully!')
else:
    print('No New Nodes Found, Skipped Node Creation!')

# Print node creation query summary after query processing
hf.query_summary(nodeCreationStats)


print('Creating Relationships...')

# Relationship creation query execution
relationshipCreation = dei_task.run(relationship_creation_query[:-5]+' RETURN rel')

# Relationship creation query stats
relationshipCreationStats = relationshipCreation.stats()

# Relationship creation status message
isNewlyCreated = relationshipCreationStats.relationships_created 
if isNewlyCreated:
    print('Relationships Created Successfully!')
else:
    print('No New Relationships Found, Skipped Relationship Creation!')

# Print relationship creation query summary after query processing
hf.query_summary(relationshipCreationStats)


f.close()
