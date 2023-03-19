import psycopg2
from random import randint

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

def start_experiment():
    # Each thread probably needs a separate connection (?) for the transactions to be isolated on programme level?
    # because https://www.psycopg.org/docs/cursor.html 'Cursors created from the same connection are not isolated' ??
    sum_b(3)
    swap_b(3, 1)
    print('***** Done *****')


def sum_b(isolation_level):
    conn = get_conn()
    conn.set_isolation_level(isolation_level)
    cur = conn.cursor()
    cur.execute(f"SELECT SUM(b) FROM {table_name}")
    row = cur.fetchone()
    result = row[0]
    cur.close()
    conn.close()
    return result

# new connection created for each thread
def swap_b(isolation_level, first_id):
    second_id = first_id + 1
    conn = get_conn()
    conn.set_isolation_level(isolation_level)
    cur = conn.cursor()

    cur.execute(f"UPDATE {table_name} SET b = b - 100 WHERE a = {first_id}")
    cur.execute(f"UPDATE {table_name} SET b = b + 100 where a = {second_id}")

    # cur.execute(f"SELECT b FROM {table_name} WHERE a = {first_id} OR a = {second_id} ORDER BY a")
    # rows = [row[0] for row in cur.fetchall()]
    # first_b, second_b = rows
    # cur.execute(f"UPDATE {table_name} SET b = {second_b} WHERE a = {first_id}")
    # cur.execute(f"UPDATE {table_name} SET b = {first_b} WHERE a = {second_id}")

    conn.commit()
    cur.close()
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