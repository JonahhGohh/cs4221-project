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

table_name = "nonrepeatable_read_accounts"


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





