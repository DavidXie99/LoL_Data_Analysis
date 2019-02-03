import sqlite3
from tinydb import TinyDB, Query
import db_config as dc
import query_builder as qb

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

def sql_bldr(sql_string,
             exe=False):
    if exe:
        execute()
        return
    
    global sql_transacts
    if type(sql_string) == str:
        sql_transacts.append(sql_string)
    elif type(sql_string) == list:
        sql_transacts += sql_string
    if len(sql_transacts) >= 10000:
        execute()

def init_tables(db_schema):
    for table_schem in db_schema:
        c.execute(qb.create_query(table_schem))
    conn.commit()

def obj_parse(data,
              obj_schema,
              file_name='',
              to_execute=True):
    query_array = []
    try:
        for table_schema in obj_schema:
            query_array += table_schema['func'](data,table_schema['sch'])
    except:
        print('Exception occurred generating SQL for:',file_name)
        return []
    if to_execute:
        sql_bldr(query_array)
    return query_array

def select_ids(table_name,
               ids,
               distinct=True):
    c.execute(qb.select_query(table_name,
                              ids,
                              distinct=distinct))
    return

def cleanup():
    execute()
    c.close()
    conn.close()


if __name__ == '__main__':
    from os import walk
    import json
    import secrets as s

    num_processed = 0
    init_conn(s.data_path + s.db_name)
    init_tables(dc.matches_schem)
    select_ids('matches', ['game_id'])
    sql_rows = {row[0] for row in c}

    seen_matches = set()
    ad1 = seen_matches.add
    for row in sql_rows:
        ad1(str(row[0]) + '.json')

    files = []
    for r,d,f in walk(s.raw_data_path):
        files += [fl for fl in f if (fl not in seen_matches)]
    
        
    for jf in files:
        with open(s.raw_data_path + jf) as infile:
            data = json.load(infile)

            obj_parse(data, dc.matches_schem, jf)
        num_processed += 1
        if not num_processed%1000:
            print('Matches processed',num_processed)

    cleanup()
    
    
