import psycopg2
import psycopg2.extras

class psql_cur:
    """docstring for psql_cur."""
    def __init__(self, dbname, user, pw):
        self.conn = psycopg2.connect('dbname={0} user={1} password={2}'.format(dbname,user,pw))
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


    def close_dbconnect(self, commit=False):
        """docstring for close_dbconnect."""

        if commit : self.conn.commit()
        self.cur.close()
        self.conn.close()
        pass
