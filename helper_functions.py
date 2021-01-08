import pandas as pd
from tabulate import tabulate

def query_summary(nodeCreationStats):
    '''
        Formats and prints the query summary
            Parameters:
                nodeCreationStats (dict): A dictionary containing query summary
            Returns:
                void
    '''
    print('~'*50)
    print(' '*18 + 'Query Summary')
    print('~'*50)
    df = pd.DataFrame({'Action': nodeCreationStats.keys(), 'Count': nodeCreationStats.values()}).values.tolist() 
    print(tabulate(df, headers = ['Action', 'Count'], tablefmt = 'psql'))
    print('-'*50)


def pascal_to_snake(pascal):
    '''
        Converts pascal case string to snake case and returns it
            Parameters:
                pascal (str): A string in pascal case
            Returns:
                snake (str): A string in snake case
    '''
    snake = ''.join(['_'+i.lower() if i.isupper() else i for i in pascal]).lstrip('_')
    return snake


def get_elements(json, id_obj_arr):
    '''
        Extracts and returns set of all the innermost field value for a given key
            Parameters:
                json (dict): A dictionary containing json data
                id_obj_arr (list): A list containing path to the key
            Returns:
                result (set): A set of value from json for given key
    '''
    if id_obj_arr.count('0-N') == 0:
        for id_obj in id_obj_arr:
            json = json[id_obj]
        return {json}
    else:
        result = set()
        for idx, id_obj in enumerate(id_obj_arr):
            if id_obj != '0-N':
                json = json[id_obj]
            else:
                id_obj_arr = id_obj_arr[(idx + 1): ]
                break
        for element in json:
            result = result.union(get_elements(element, id_obj_arr))
        return result
        
def resolve_node_prop_values(unresolved_query):
    '''
        Accepts a query which creates node with property value as string instead of keywords and return a query which updates the property to keywords
            Parameters:
                unresolved_query (str): A string containing unresolved query
            Returns:
                resolved_query (str): A string containing resolved query
    '''
    resolved_query = unresolved_query + '''
        WITH null as nothing
        MATCH (node)
	    WITH DISTINCT keys(node) AS keys
        WITH DISTINCT keys as dKeys
        UNWIND dKeys as props
        WITH COLLECT(DISTINCT props) as allProps
        UNWIND allProps as prop
        MATCH (updateNode)
        WHERE updateNode[prop] = 'null'
        WITH updateNode, prop
        CALL
        	apoc.create.setProperty(updateNode, prop, null)
        YIELD node
        
        MATCH (node)
	    WITH DISTINCT keys(node) AS keys
        WITH DISTINCT keys as dKeys
        UNWIND dKeys as props
        WITH COLLECT(DISTINCT props) as allProps
        UNWIND allProps as prop
        MATCH (updateNode)
        WHERE updateNode[prop] = 'True'
        WITH updateNode, prop
        CALL
        	apoc.create.setProperty(updateNode, prop, true)
        YIELD node
        RETURN node
    '''
    return resolved_query
    
    
