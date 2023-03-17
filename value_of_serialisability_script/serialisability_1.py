import psycopg2
from random import randint

# To setup:
# Step 1: Activate virtual env, e.g. by running 'venv\Scripts\Activate'
# Step 2: Run 'pip install -r requirements.txt' to install psycopg

# Step 3: Install Postgresql locally on your machine, create a database and user, then configure the db settings below
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "password": "1234"
}

table_name = "serialisability_1"

# Step 4: Run this python file, check table is successfully created in your database with rows seeded automatically

def start_experiment():
    # Each thread probably needs a separate connection (?) for the transactions to be isolated on programme level?
    # because https://www.psycopg.org/docs/cursor.html 'Cursors created from the same connection are not isolated' ??
    sum_b()


def sum_b():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT SUM(b) FROM {table_name}")
    result = cur[0][0]
    cur.close()
    conn.close()
    return result

# new connection created for each thread
def swap_b(id, length):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE {table_name} SET b = b - 100 WHERE a = {id}")
    cur.execute(f"UPDATE {table_name} SET b = b + 100 where a = {(id + 1) % length}")
    cur.close()
    conn.close()

def setup_db():
    conn = get_conn()
    cur = conn.cursor()
    schema_file = get_file("schema.sql")
    seed_file = get_file("seed.sql")

    try:
        cur.execute(schema_file)
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


def main():
    setup_db()
    start_experiment()


if __name__ == '__main__':
    main()