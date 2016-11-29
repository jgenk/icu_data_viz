
import psycopg2 as p2
from psycopg2 import extras

class psqlstr(object):
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"

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
    
    def create_select(self,table,columns):
        if isinstance(columns,list): columns =','.join(columns)
        return 'SELECT {columns} FROM {table}'.format(columns=columns, table=table)

    def simple_select(self,table,columns,where=None,param=[],fetchall=True):
        command = self.create_select(table,columns)
        if where is not None: command += ' WHERE {where_case}'.format(where_case=where)
        return self.execute(command,param,fetchall)
    
    def execute(self, command,param=[],fetchall=True):
        self.cur.execute(command,param)
        if fetchall: return self.cur.fetchall()
        return self.cur.query   
        

def open_db(dbname, user, password):
    """
    Opens DB connection. sets namespace to schema if desired
        returns cur, conn
    """
    conn = p2.connect("dbname=" + dbname + " user=" + user + " password=" + password)
    cur = conn.cursor(cursor_factory=extras.DictCursor)

    return conn,cur

def set_namespace(cur,schema):
    cur.execute('SET search_path TO {0}'.format(schema))
    return cur.query

def get_rows_for_ID(cur, ID, table_name, id_col_nm, columns='*' ):
    """Given an ID and table name, identify the ID column and
        retrieve all rows in that table related to the subject_ID."""
    cur.execute(' '.join([psqlstr.SELECT, columns,
                         psqlstr.FROM, table_name,
                         psqlstr.WHERE, table_name + '.' + id_col_nm, "=", '\'{0}\''.format(ID)]))
    return cur.fetchall()

