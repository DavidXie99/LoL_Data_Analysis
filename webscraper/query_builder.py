

uppercase = {**{chr(i+65):chr(i+97) for i in range(26)}, **{'-':'_'}}

py_to_lite = {int : 'INTEGER',
              str : 'TEXT',
              float : 'REAL',
              bool : 'INTEGER'}

def camelToSnake(string):
    #return string
    new_string = ''
    for c in string:
        if c in uppercase:
            new_string += '_'
            new_string += uppercase[c]
        else:
            new_string += c
    return new_string

def boolToInt(val):
    if type(val) == bool:
        return int(val)
    if type(val) == str:
        return "'"+val+"'"
    return val

def insert_query(obj,
                 schema,
                 table_name,
                 add_key=dict(),
                 t=dict):
    if t == dict:
        sch = []
        values = []
        ap1 = sch.append
        ap2 = values.append

        for k in add_key:
            ap1(camelToSnake(k))
            ap2(str(add_key[k]))
        
        for key in schema:
            if type(schema[key]) == type:
                ap1(camelToSnake(key))
                try:
                    ap2(str(boolToInt(obj[key])))
                except:
                    ap2('null')
            else:
                for k in schema[key]:
                    ap1(camelToSnake(key+k))
                    try:
                        ap2(str(boolToInt(obj[key][k])))
                    except:
                        ap2('null')
        query = '''INSERT INTO {table} ({schem}) VALUES ({vals})'''.format(table=table_name,
                                                                           schem=','.join(sch),
                                                                           vals=','.join(values))
        return query
    if t == list:
        sch = [k for k in add_key]
        values = []
        row = []
        ap1 = sch.append
        ap2 = values.append

        for key in schema:
            if type(schema[key]) == type:
                ap1(camelToSnake(key))
            else:
                for k in schema[key]:
                    ap1(camelToSnake(k))

        for item in obj:
            row = [str(add_key[k]) for k in add_key]
            for key in schema:
                if type(schema[key]) == type:
                    try:
                        row.append(str(boolToInt(item[key])))
                    except:
                        row.append('null')
                else:
                    for k in schema[key]:
                        row.append(str(boolToInt(item[key][k])))
            ap2('({vals})'.format(vals=','.join(row)))

        query = '''INSERT INTO {table} ({schem}) VALUES {rows}'''.format(table=table_name,
                                                                         schem=','.join(sch),
                                                                         rows=','.join(values))
        return query
    return None

def create_query(table_schem):
    sch = []
    ap1 = sch.append

    for k in table_schem['add_key']:
        ap1(camelToSnake(k) + ' ' + py_to_lite[table_schem['add_key'][k]])
        
    num_dicts = 0
    for key in table_schem['sch']:
        num_dicts += int(type(table_schem['sch'][key]) == dict)
        if num_dicts > 1:
            break
    
    for key in table_schem['sch']:
        if type(table_schem['sch'][key]) == type:
            ap1(camelToSnake(key) + ' ' + py_to_lite[table_schem['sch'][key]])
        else:
            for k in table_schem['sch'][key]:
                keyname = k
                if num_dicts > 1:
                    keyname = key + k
                ap1(camelToSnake(keyname) + ' ' + py_to_lite[table_schem['sch'][key][k]])
    query = '''CREATE TABLE IF NOT EXISTS {table} ({schem})'''.format(table=table_schem['name'],
                                                                      schem=','.join(sch))
    return query
                             



