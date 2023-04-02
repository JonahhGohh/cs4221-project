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


def get_first_name_execution_plan():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("EXPLAIN ANALYSE SELECT * FROM composite_index_experiment WHERE first_name = 'Fionna' ")
        row = cur.fetchall()
        # print(row)
        print("Statistics of first query (with first_name)")
        print("Strategy: ", row[0])
        print("Planning time: ", row[-2])
        print("Execution time: ", row[-1])
    except Exception as e:
        print('Error retrieving data', e)



def get_first_name_last_name_not_using_index_execution_plan():
    conn = get_conn()
    cur = conn.cursor()

    try:
        # absence of equality condition 
        cur.execute("EXPLAIN ANALYSE SELECT * FROM composite_index_experiment WHERE first_name > 'G' and last_name > 'G'")
        row = cur.fetchall()
        # print(row)
        print("Statistics of third query (not using equality operator on index)")
        print("Strategy: ", row[0])
        print("Planning time: ", row[-2])
        print("Execution time: ", row[-1])
    except Exception as e:
        print('Error retrieving data', e)

def get_first_name_last_name_using_index_execution_plan():
    conn = get_conn()
    cur = conn.cursor()

    try:
        # presence of equality condition 
        cur.execute("EXPLAIN ANALYSE SELECT * FROM composite_index_experiment WHERE first_name = 'G' and last_name > 'G'")
        row = cur.fetchall()
        # print(row)
        print("Statistics of fourth query (using equality operator on index)")
        print("Strategy: ", row[0])
        print("Planning time: ", row[-2])
        print("Execution time: ", row[-1])

    except Exception as e:
        print('Error retrieving data', e)


def get_last_name_execution_plan():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("EXPLAIN ANALYSE SELECT * FROM composite_index_experiment WHERE last_name = 'Levison' ")
        row = cur.fetchall()
        print("Statistics of second query (with last_name)")
        print("Strategy: ", row[0])
        print("Planning time: ", row[-2])
        print("Execution time: ", row[-1])

    except Exception as e:
        print('Error retrieving data', e)


def get_conn():
    return psycopg2.connect(**db_config)


def get_file(filename):
    f = open(filename, "r")
    file = f.read()
    f.close()

    return file





