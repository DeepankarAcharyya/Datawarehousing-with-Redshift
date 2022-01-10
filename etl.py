import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_tables_name, insert_tables_name


def load_staging_tables(cur, conn):
    """
    Function to load data from the files stored in S3 to the staging tables.
    """
    print("Start : Loading data into the staging tables.")
    
    i=0
    for query in copy_table_queries:
        #print(query)
        cur.execute(query)
        conn.commit()
        print("Completed for table : ",copy_tables_name[i] )
        i=i+1
        
        
    print("Completed : Data loaded into the staging tables.")

def insert_tables(cur, conn):
    """
    Insert data from the staging tables to the final tables.
    """
    print("Loading data into the final tables.")
    
    i=0
    for query in insert_table_queries:
        #print(query)
        cur.execute(query)
        conn.commit()
        print("Completed for table : ", insert_tables_name[i] )
        i=i+1
        
    
    print("Completed : Data loaded into the final tables.")

def main():
    """
    Function to extract the data from the files present in S3, transform it using the staging tables and load it into the fact & dimensional tables for analysis.
    """
    print("Starting the script to insert the data into the tables.")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Config file loaded. Establishing connection.")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to the redshift cluster.")

    try:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception as e:
        print("Exception encountered : ",e)

    conn.close()


if __name__ == "__main__":
    main()