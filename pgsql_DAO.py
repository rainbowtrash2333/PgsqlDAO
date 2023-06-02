#!/usr/bin/python

from configparser import ConfigParser
import psycopg2

def decorator_error(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    return wrapper
    
class DAO:
    def __init__(self, filename='database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        self.params =db
        


    @decorator_error
    def connect(self):
        conn = None
        print('Connecting to the PostgreSQL database...')
        self.conn = psycopg2.connect(**self.params)
        self.cur = self.conn.cursor()
            

    @decorator_error
    def create_table(self,table_name):
        exists = False
        self.cur.execute("select exists(select relname from pg_class where relname='" + table_name + "')")
        exists = self.cur.fetchone()[0]
        if not exists:
            print("create table")
            f = open("create_table.sql", "r")
            command=f.read()
            self.cur.execute(command)
    
    @decorator_error
    def insert(self,table_name,record):
        postgres_insert_query = """ INSERT INTO """+table_name+""" (ID, MODEL, PRICE) VALUES (%s,%s,%s)"""
        self.cur.execute(postgres_insert_query, record)
    
    @decorator_error
    def commit(self):
        self.cur.commit()
    
    
    @decorator_error
    def close(self):
        # close the communication with the PostgreSQL
        if self.conn:
            self.cur.close()
            self.conn.close()
            
if __name__ == "__main__":
    dao = DAO()
    dao.connect()
    dao.create_table('TABLENAME')
    dao.close()