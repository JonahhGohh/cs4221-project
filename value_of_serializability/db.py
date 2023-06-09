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

def sum_b(isolation_level):
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

# new connection created for each thread
def swap_b(isolation_level, first_id):
    second_id = first_id + 1
    conn = get_conn()

    retries = 0
    while True:
        conn.set_isolation_level(isolation_level)
        cur = conn.cursor()
        if retries >= 10:
            break
        try:
            cur.execute(f"UPDATE {table_name} SET b = b - 100 WHERE a = {first_id}")
            cur.execute(f"UPDATE {table_name} SET b = b + 100 where a = {second_id}")
            

            # cur.execute(f"SELECT b FROM {table_name} WHERE a = {first_id} OR a = {second_id} ORDER BY a")
            # rows = [row[0] for row in cur.fetchall()]
            # first_b, second_b = rows
            # cur.execute(f"UPDATE {table_name} SET b = {second_b} WHERE a = {first_id}")
            # cur.execute(f"UPDATE {table_name} SET b = {first_b} WHERE a = {second_id}")
            conn.commit()
            cur.close()
            break

        except Exception as error:
            print('swap_b() ERROR')
            retries += 1 
            print(error)
            print('\n')
            cur.close()
            conn.rollback()

    conn.close()


def setup_db():
    conn = get_conn()
    cur = conn.cursor()
    schema_file = get_file("schema.sql")
    seed_file = get_file("seed.sql")

    try:
        cur.execute(schema_file)
        conn.commit()
        cur.execute(seed_file)
        conn.commit()
    except psycopg2.errors.DuplicateTable:
        print('Experiment table already created, skipping...')
    except Exception as e:
        print('Error creating Experiment table', e)
    
    cur.close()
    conn.close()


def get_conn():
    return psycopg2.connect(**db_config)


def get_file(filename):
    f = open(filename, "r")
    file = f.read()
    f.close()

    return file





