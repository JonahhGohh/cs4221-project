import psycopg2
from psycopg2 import pool
from math import floor
# To setup, follow README.md steps

# Connecting to postgresql in container
connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=2,
    dbname="postgres",
    user="postgres",
    password="1234",
    host="db",
    port="5432"
)

table_name = "indexing"

# consider: what if index is created at the start of table creation
def query_age_range(num_of_queries: int):
  conn = get_conn()
  cur = conn.cursor()
  lower_bound = 0
  upper_bound = 10
  # rotate age range in query to prevent result caching from affecting experimental results
  for i in range(num_of_queries):
    if (lower_bound >= 100):
        lower_bound = 0
        upper_bound = 10
    cur.execute(f"SELECT id FROM {table_name} WHERE age >= {lower_bound} AND age <= {upper_bound}")
    lower_bound += 10
    upper_bound += 10
  cur.close()
  close_conn(conn)
  
def insert_new_rows(num_of_insertions: int, end_id: int):
  conn = get_conn()
  cur = conn.cursor()
  for i in range(num_of_insertions):
    cur.execute(f"INSERT INTO indexing(id, age) VALUES ({end_id + i + 1}, {floor(num_of_insertions % 100)})")
  
  conn.commit()
  cur.close()
  close_conn(conn)

def setup_db(has_index: bool = False):
    conn = get_conn()
    cur = conn.cursor()
    schema_file = get_file("schema.sql")
    seed_file = get_file("seed.sql")

    try:
        cur.execute(schema_file)
        conn.commit()
        if has_index:
          # BTREE data structure is built if indexing data structure is not specifiied
          # BTREE is the most efficient for range querying
          # age is sorted in ascending order by default
          cur.execute("CREATE INDEX age_index ON indexing (age)")
          conn.commit()
        cur.execute(seed_file)
        conn.commit()
    except psycopg2.errors.DuplicateTable:
        print('Experiment table already created, skipping...')
    except Exception as e:
        print('Error creating Experiment table', e)

    cur.close()
    close_conn(conn)


def get_conn():
  return connection_pool.getconn()
  
def close_conn(connection):
  connection_pool.putconn(connection)


def get_file(filename):
    f = open(filename, "r")
    file = f.read()
    f.close()

    return file
