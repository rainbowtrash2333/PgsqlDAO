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
    def is_table_exists(self,table_name):
        exists = False
        self.cur.execute("select exists(select relname from pg_class where relname='" + table_name + "')")
        exists = self.cur.fetchone()[0]
        return exists

    @decorator_error
    def create_table(self,table_name):
        if not self.is_table_exists(table_name):
            print("create table: "+table_name)
            f = open("create_table.sql", "r")
            command=f.read()
            self.cur.execute(command)
    
    @decorator_error
    def drop_table(self,table_name):
        if not self.is_table_exists(table_name):
            print("drop table: "+table_name)
            self.cur.execute("DROP TABLE IF EXISTS "+table_name)
    
    # @decorator_error
    def insert(self,table_name,record):
        postgres_insert_query =""" INSERT INTO """+table_name+""" (ID, PASSWD) VALUES (%s,%s)"""
        self.cur.execute(postgres_insert_query, record)
    
    def select_all(self,table_name, row_count=100):
        postgres_select_query = """ Select * From """+table_name+""" LIMIT """+str(row_count)
        self.cur.execute(postgres_select_query)
        records = self.cur.fetchall()
        print(records)
    
    @decorator_error
    def commit(self):
        self.conn.commit()
    
    
    @decorator_error
    def close(self):
        # close the communication with the PostgreSQL
        if self.conn:
            self.cur.close()
            self.conn.close()
            
    def innert_data_from_txt(self,table_name,filepath='data.txt',encode='utf-8',ignore_first_line = True):
        with open(file=filepath,mode='r',encoding=encode ) as data:
            line = data.readline()
            if ignore_first_line:
                line = data.readline()
            while line is not None and line !="":
                record=tuple(line.strip().split("|"))
                self.insert(table_name=table_name,record=record)
                line = data.readline()
                
            
if __name__ == "__main__":
    table_name = 'USERS'
    dao = DAO()
    dao.connect()
    dao.drop_table(table_name)
    dao.create_table(table_name)
    dao.insert(table_name,("Tda","fds123"))
    dao.innert_data_from_txt(table_name=table_name,filepath='data.txt',ignore_first_line=True)
    dao.commit()
    dao.select_all(table_name)
    dao.close()