import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''
    drop_tables function executes the queries that DROP all tables.

    PARAMS:
        cur: cursor
        conn: connection object

'''
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

'''
    create_tables function executes the queries that CREATE all tables.

    PARAMS:
        cur: cursor
        conn: connection object

'''
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

'''
    main function creates the connection, reads the credential files (dwh.cfg) and executes de drop and create functions

'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Connected")

    print("Dropping tables...")
    drop_tables(cur, conn)
    print("... COMPLETE")

    print("Creating tables...")
    create_tables(cur, conn)
    print("... COMPLETE")


    

    conn.close() 


if __name__ == "__main__":
    main()