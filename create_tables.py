import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_tables_name


def drop_tables(cur, conn):
    '''
    Function to drop the existing tables.
    '''
    print("Dropping the existing tables.")
    
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def create_tables(cur, conn):
    '''
    Function to create the required tables.
    '''
    print("Creating new tables.")
    i=0
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Table created : ", create_tables_name[i])
            i=i+1
        except Exception as e:
            print(e)


def main():
    '''
    Function to reset the database, by dropping the existing tables and creating new tables.
    '''
    
    print("Starting the script to create the required tables.")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Config file loaded. Establishing connection.")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to the redshift cluster.")
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
    print("Script Done.")


if __name__ == "__main__":
    main()
    