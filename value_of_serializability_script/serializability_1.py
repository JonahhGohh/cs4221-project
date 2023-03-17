import psycopg2

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
    sum_a()
    print('***** Done *****')


def sum_a():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT SUM(b) FROM {table_name}")
    for row in cur:
        print('sum', row[0])
    cur.close()
    conn.close()
    return


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