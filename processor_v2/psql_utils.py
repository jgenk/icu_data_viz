
import psycopg2 as p2
import psycopg2.extras

class DBHelper(object):

    def __init__(self, dbname,user,password, schema=None):
        self.conn,self.cur=open_db(dbname,user,password,)
        self.execute_log=[]
        if schema is not None: self.set_namespace(schema)

    def set_namespace(self,schema):
        return set_namespace(self.cur, schema)

    def close(self,should_commit=True):
        if should_commit: self.conn.commit()
        self.cur.close()
        self.conn.close()
        return True

    def simple_select(self,table,columns,where=None,fetchall=True):
        if isinstance(columns,list): columns =','.join(columns)
        command = 'SELECT {columns} FROM {table}'.format(columns=columns, table=table)
        if where is not None: command += ' WHERE {where_case}'.format(where_case=where)
        self.cur.execute(command)
        if fetchall: return self.cur.fetchall()
        return self.cur.query


def open_db(dbname, user, password):
    """
    Opens DB connection. sets namespace to schema if desired
        returns cur, conn
    """
    conn = p2.connect("dbname=" + dbname + " user=" + user + " password=" + password)
    cur = conn.cursor(cursor_factory=p2.extras.DictCursor)

    return conn,cur

def set_namespace(cur,schema):
    cur.execute('SET search_path TO {0}'.format(schema))
    return cur.query

def get_rows_for_ID(cur, ID, table_name, id_col_nm, columns='*' ):
    """Given an ID and table name, identify the ID column and
        retrieve all rows in that table related to the subject_ID."""
    cur.execute(' '.join([psql_str.SELECT, columns,
                         psql_str.FROM, table_name,
                         psql_str.WHERE, table_name + '.' + id_col_nm, "=", '\'{0}\''.format(ID)]))
    return cur.fetchall()
