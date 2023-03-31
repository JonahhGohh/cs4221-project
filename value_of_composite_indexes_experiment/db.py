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

def setup_db():
    conn = get_conn()
    cur = conn.cursor()
    schema_file = get_file("schema.sql")

    try:
        cur.execute(schema_file)
        conn.commit()
    except psycopg2.errors.DuplicateTable:
        print('Experiment table already created, skipping...')
    except Exception as e:
        print('Error creating Experiment table', e)

    conn.commit()
    cur.close()
    conn.close()


def get_first_name():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM composite_index_experiment WHERE first_name = 'Fionna' ")
    except Exception as e:
        print('Error retrieving data', e)


def get_first_name_execution_plan():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("EXPLAIN ANALYSE SELECT * FROM composite_index_experiment WHERE first_name = 'Fionna' ")
        row = cur.fetchall()
        # print(row)
        print("Statistics of first query (with first_name)")
        print("Planning time: ", row[5])
        print("Execution time: ", row[6])
    except Exception as e:
        print('Error retrieving data', e)


def get_last_name():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM composite_index_experiment WHERE last_name = 'Levison' ")
    except Exception as e:
        print('Error retrieving data', e)

def get_last_name_execution_plan():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("EXPLAIN ANALYSE SELECT * FROM composite_index_experiment WHERE last_name = 'Levison' ")
        row = cur.fetchall()
        # print(row)
        print("Statistics of second query (with last_name)")
        print("Planning time: ", row[3])
        print("Execution time: ", row[4])

    except Exception as e:
        print('Error retrieving data', e)


def get_conn():
    return psycopg2.connect(**db_config)


def get_file(filename):
    f = open(filename, "r")
    file = f.read()
    f.close()

    return file





