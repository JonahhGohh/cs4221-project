import psycopg2
import time

# To setup, follow README.md steps

# Connecting to postgresql in container
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "db",
    "password": "1234",
    "port": "5432"
}

table_name = "serializability_1"

def count_b(isolation_level, table_name = "serializability_1"):
    conn = get_conn()
    result = 0 # If serialization error, return 0 to cause wrong result instead of crashing the script

    retries = 0
    while True:
        conn.set_isolation_level(isolation_level)
        cur = conn.cursor()
        if retries >= 10:
            break
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row = cur.fetchone()
            result = row[0]
            cur.close()
            break

        except Exception as error:
            print('count_b() ERROR')
            retries += 1
            print(error)
            print('\n')
            cur.close()
            conn.rollback()
    
    conn.close()
    return result

def sum_b(isolation_level, table_name = "serializability_1"):
    conn = get_conn()
    result = 0 # If serialization error, return 0 to cause wrong result instead of crashing the script

    retries = 0
    while True:
        conn.set_isolation_level(isolation_level)
        cur = conn.cursor()
        if retries >= 10:
            break
        try:
            cur.execute(f"SELECT SUM(b) FROM {table_name}")
            row = cur.fetchone()
            result = row[0]
            cur.close()
            break

        except Exception as error:
            print('sum_b() ERROR')
            retries += 1
            print(error)
            print('\n')
            cur.close()
            conn.rollback()
    
    conn.close()
    return result

def sum_insert(isolation_level, insert_id, table_name = 'serializability_2'):
    conn = get_conn()
    result = 0

    retries = 0
    while True:
        conn.set_isolation_level(isolation_level)
        cur = conn.cursor()
        if retries >= 10:
            break
        try:
            cur.execute(f'SELECT SUM(b) FROM {table_name}')
            row = cur.fetchone()
            result = row[0]
            cur.execute(f'INSERT INTO {table_name} VALUES ({insert_id}, {result})')
            conn.commit()
            cur.close()
            break

        except Exception as error:
            print('sum_insert() ERROR')
            retries += 1 
            print(error)
            print('\n')
            cur.close()
            conn.rollback()
        
        conn.close()
        return result

def setup_db():
    conn = get_conn()
    cur = conn.cursor()
    schema_file = get_file("schema.sql")
    seed_file = get_file("seed.sql")

    try:
        cur.execute(schema_file)
        conn.commit()
        cur.execute(seed_file)
    except psycopg2.errors.DuplicateTable:
        print('Experiment table already created, skipping...')
    except Exception as e:
        print('Error creating Experiment table', e)

    conn.commit()
    cur.close()
    conn.close()


def get_conn():
    return psycopg2.connect(**db_config)


def get_file(filename):
    f = open(filename, "r")
    file = f.read()
    f.close()

    return file





