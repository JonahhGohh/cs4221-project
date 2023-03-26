import psycopg2
import sys
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


def withdrawal_transaction(isolation_level, select_query_type, withdrawal_amount):
    conn = get_conn()

    retries = 0
    while True:
        conn.set_isolation_level(isolation_level)
        cur = conn.cursor()

        # if retries >= 10:
        #     break
        try:
            cur.execute(
                f"SELECT balance FROM {table_name} WHERE id = 1 {select_query_type}")
            row = cur.fetchone()
            balance = row[0]
            if balance >= withdrawal_amount:
                cur.execute(
                    f"UPDATE {table_name} SET balance = balance - {withdrawal_amount} WHERE id = 1")
                conn.commit()
            cur.close()
            break
        except Exception as error:
            print('withdrawal_transaction ERROR')
            retries += 1
            print(error)
            print('\n')
            cur.close()
            conn.rollback()

    conn.close()
    return retries


def find_end_balance():
    conn = get_conn()
    cur = conn.cursor()
    balance = None

    try:
        cur.execute(f"SELECT balance FROM {table_name} WHERE id = 1")
        row = cur.fetchone()
        balance = row[0]

    except Exception as error:
        print('Unable to find end balance due to error ', error)

    cur.close()
    conn.close()
    return balance


def reset_balance(start_balance):
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            f"UPDATE {table_name} SET balance = {start_balance} WHERE id = 1")
        conn.commit()

    except Exception as error:
        print('Unable to reset balance due to error, exiting... ', error)
        sys.exit()

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
