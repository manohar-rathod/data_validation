import os


def convert_type_in_hash(di_ct):
    for key, value in di_ct.items():
        di_ct[key] = str(value)
    return di_ct


def make_directory(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as error:
        print(error)


def read_sql_file(query_file):
    query = open(query_file, 'r')
    read_query = query.read()
    query.close()
    return read_query.upper()
