import sqlite3
from tinydb import TinyDB, Query
import db_config as dc

sql_transacts = []
conn = None
c = None

def init_conn(db_name):
    global conn, c
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

def execute():
    global sql_transacts
    for s in sql_transacts:
        try:
            c.execute(s)
        except:
            print('Error executing sql:\n',s)
    conn.commit()
    sql_transacts = []

def sql_bldr(sql_string):
    global sql_transacts
    if type(sql_string) == str:
        sql_transacts.append(sql_string)
    elif type(sql_string) == list:
        sql_transacts += sql_string
    if len(sql_transacts) >= 10000:
        execute()

def obj_parse(data,
              obj_schema,
              to_execute=True):
    query_array = []
    try:
        for table_schema in obj_schema:
            query_array += table_schema['func'](data,table_schema['sch'])
    except:
        print('Exception occurred generating SQL for:',data['gameId'])
        return []
    if to_execute:
        sql_bldr(query_array)
    return query_array

def cleanup():
    execute()
    c.close()
    conn.close()


if __name__ == '__main__':
    from os import walk
    import json
    import query_builder as qb
    import secrets as s

    num_processed = 0
    init_conn('MatchTest.db')

    files = []
    for r,d,f in walk(s.data_path):
        files += f

    for table_schem in dc.matches_schem:
        sql_bldr(qb.create_query(table_schem))
        
    for jf in files:
        with open(s.data_path + jf) as infile:
            data = json.load(infile)

            obj_parse(data, dc.matches_schem)
        num_processed += 1
        if not num_processed%1000:
            print('Matches processed',num_processed)

    cleanup()
    
    
