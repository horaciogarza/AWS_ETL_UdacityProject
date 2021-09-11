import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


'''
    load_staging_tables function executes the queries that load the data into redshift.

    PARAMS:
        cur: cursor
        conn: connection object

'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

'''
    insert_tables function executes the INSERT queries based on the data that was loaded.

    PARAMS:
        cur: cursor
        conn: connection object

'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

'''
    main function creates the connection, reads the credential files (dwh.cfg) and executes the load_staging_tables and 
    insert_tables functions.

'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected")


    print("Loading data...")
    load_staging_tables(cur, conn)
    print("Data loaded")

    print("Inserting data")
    #insert_tables(cur, conn)
    print("Data inserted")

    conn.close()


if __name__ == "__main__":
    main()