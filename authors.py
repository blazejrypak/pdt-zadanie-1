import gzip
import psycopg2
import json
from db import connect_db
from logger import Logger


def not_null_str(param):
    if param:
        return str(param).encode('utf-8').decode('utf-8').replace("\x00", "\uFFFD")
    else:
        return param


def parse_author(author):
    public_metrics = author.get('public_metrics', {})
    return (author['id'], not_null_str(author.get('name', None)),
            not_null_str(author.get('username', None)),
            not_null_str(author.get('description', None)),
            public_metrics.get('followers_count', None),
            public_metrics.get('following_count', None),
            public_metrics.get('tweet_count', None),
            public_metrics.get('listed_count', None))


def insert_records(conn, cur, logger, parsed_authors):
    authors_args = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s)", x).decode(
        "utf-8") for x in parsed_authors)
    if authors_args:
        cur.execute("INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES " +
                    authors_args + " ON CONFLICT DO NOTHING;")
        conn.commit()
        logger.log()


def load_authors():
    conn, cur = connect_db()
    logger = Logger('report_file_authors')
    with gzip.open('authors.jsonl.gz', 'r') as file:
        parsed_authors = []
        for line in file:
            line = line.decode('utf-8')
            author = parse_author(json.loads(line))
            parsed_authors.append(author)
            if len(parsed_authors) >= 200000:
                insert_records(conn, cur, logger, parsed_authors)
                parsed_authors = []

        if len(parsed_authors):
            insert_records(conn, cur, logger, parsed_authors)

    cur.close()
    conn.close()
