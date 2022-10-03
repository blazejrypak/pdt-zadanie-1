import psycopg2

db = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'twitter',
    'user': 'postgres',
    'password': 'postgres'
}


def connect_db():
    conn = psycopg2.connect(
        f"host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']} password={db['password']}",)
    cur = conn.cursor()
    return conn, cur
