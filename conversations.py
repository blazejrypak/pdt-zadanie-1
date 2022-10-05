import gzip
import json
from db import connect_db
from logger import Logger


def not_null_str(param):
    if param:
        return str(param).encode('utf-8').decode('utf-8').replace("\x00", "\uFFFD")
    else:
        return param


def parse_conversation(data):
    public_metrics = data.get('public_metrics', {})
    return (data['id'], data['author_id'], not_null_str(data['text']), data['possibly_sensitive'], data['lang'],
            not_null_str(data['source']),
            public_metrics.get('retweet_count', None),
            public_metrics.get('reply_count', None),
            public_metrics.get('like_count', None),
            public_metrics.get('quote_count', None), data['created_at'])


def parse_links(data):
    urls = data.get('entities', {}).get('urls', [])
    links = []
    for url in urls:
        if url.get('expanded_url', None):
            if (len(url['expanded_url']) <= 2048):
                links.append((url['expanded_url'], url.get('title', None),
                              url.get('description', None), data['id']))
    return links


def parse_annotations(data):
    annotations_data = data.get('entities', {}).get('annotations', [])
    annotations = []
    for annotation in annotations_data:
        annotations.append(
            (not_null_str(annotation.get('normalized_text', '')), not_null_str(
                annotation.get('type', '')), annotation.get('probability', 0.0), data['id'])
        )
    return annotations


def parse_context_annotations(data):
    context_annotations_data = data.get('context_annotations', [])
    context_annotations_domains = []
    context_annotations_entities = []
    for context_annotation in context_annotations_data:
        if context_annotation['domain'] and context_annotation['entity']:
            context_annotations_domains.append((
                context_annotation['domain']['id'],
                context_annotation['domain'].get('name', ''),
                context_annotation['domain'].get('description', ''),
                data['id']
            ))
            context_annotations_entities.append((
                context_annotation['entity']['id'],
                context_annotation['entity'].get('name', ''),
                context_annotation['entity'].get('description', ''),
                data['id']
            ))
    return context_annotations_domains, context_annotations_entities


def parse_hashtags(data):
    hashtags_data = data.get('entities', {}).get('hashtags', [])
    hashtags = []
    for hashtag in hashtags_data:
        hashtags.append(
            not_null_str(hashtag['tag']),
        )
    return hashtags


def parse_referenced_tweets(data):
    referenced_tweets_data = data.get('referenced_tweets', [])
    referenced_tweets = []
    for referenced_tweet in referenced_tweets_data:
        referenced_tweets.append(
            (referenced_tweet['id'], not_null_str(referenced_tweet['type']), data['id']))
    return referenced_tweets


def insert_records(conn, cur, logger, parsed_conversations, parsed_annotations, parsed_links, parsed_referenced_tweets, new_added_hashtags, new_added_hashtags_conversations, parsed_context_annotations_entities, parsed_context_annotations_domains):
    conversation_args = ','.join(cur.mogrify(
        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in parsed_conversations)
    if conversation_args:
        cur.execute("INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, qoute_count, created_at) VALUES " +
                    conversation_args + "  ON CONFLICT DO NOTHING;")

        for pa in parsed_annotations:
            annotations_args = ','.join(cur.mogrify(
                "(%s,%s,%s,%s)", x).decode("utf-8") for x in pa)
            if annotations_args:
                cur.execute("INSERT INTO annotations (value, type, probability, conversation_id) VALUES " +
                            annotations_args + "  ON CONFLICT DO NOTHING;")

        for pl in parsed_links:
            links_args = ','.join(cur.mogrify(
                "(%s,%s,%s,%s)", x).decode("utf-8") for x in pl)
            if links_args:
                cur.execute("INSERT INTO links (url, title, description, conversation_id) VALUES " +
                            links_args + "  ON CONFLICT DO NOTHING;")

        for prt in parsed_referenced_tweets:
            referenced_tweets_args = ','.join(cur.mogrify(
                "(%s,%s,%s)", x).decode("utf-8") for x in prt)
            if referenced_tweets_args:
                cur.execute("INSERT INTO conversation_references (parent_id, type, conversation_id) VALUES " +
                            referenced_tweets_args + "  ON CONFLICT DO NOTHING;")

        hashtags_args = ','.join(cur.mogrify(
            "(%s,%s)", x).decode("utf-8") for x in new_added_hashtags)

        if hashtags_args:
            cur.execute("INSERT INTO hashtags (id, tag) VALUES " +
                        hashtags_args + " ON CONFLICT DO NOTHING;")

        conversations_hashtags_args = ','.join(cur.mogrify(
            "(%s,%s)", x).decode("utf-8") for x in new_added_hashtags_conversations)

        if conversations_hashtags_args:
            cur.execute("INSERT INTO conversation_hashtags (conversation_id, hashtag_id) VALUES " +
                        conversations_hashtags_args + " ON CONFLICT DO NOTHING;")

        for pcad, pcae in zip(parsed_context_annotations_domains, parsed_context_annotations_entities):
            context_annotations_domains_args = ','.join(cur.mogrify(
                "(%s,%s,%s)", (x[0], x[1], x[2])).decode("utf-8") for x in pcad[0])
            if context_annotations_domains_args:
                cur.execute("INSERT INTO context_domains (id, name, description) VALUES " +
                            context_annotations_domains_args + "  ON CONFLICT DO NOTHING;")
            context_annotations_entities_args = ','.join(cur.mogrify(
                "(%s,%s,%s)", (x[0], x[1], x[2])).decode("utf-8") for x in pcae[0])
            if context_annotations_entities_args:
                cur.execute("INSERT INTO context_entities (id, name, description) VALUES " +
                            context_annotations_entities_args + "  ON CONFLICT DO NOTHING;")

            values = []
            for a, b in zip(pcad[0], pcae[0]):
                values.append((a[3], a[0], b[0]))
            values_args = ','.join(cur.mogrify(
                "(%s,%s,%s)", x).decode("utf-8") for x in values)
            if values_args:
                cur.execute("INSERT INTO context_annotations (conversation_id, context_domain_id, context_entity_id) VALUES " +
                            values_args + "  ON CONFLICT DO NOTHING;")
        conn.commit()
        logger.log()


def load_conversations():
    conn, cur = connect_db()
    logger = Logger('report_file_conversations')
    hashtags_dict = dict()
    cur.execute('SELECT id, tag FROM hashtags')
    for r in cur.fetchall():
        hashtags_dict[r[1]] = r[0]
    with gzip.open('conversations.jsonl.gz', 'r') as file:
        parsed_conversations = []
        parsed_annotations = []
        parsed_links = []
        parsed_context_annotations_domains = []
        parsed_context_annotations_entities = []
        parsed_referenced_tweets = []
        new_added_hashtags = []
        new_added_hashtags_conversations = []
        for line in file:
            data = json.loads(line)
            conversation = parse_conversation(data)
            links = parse_links(data)
            annotations = parse_annotations(data)
            context_annotations_domains, context_annotations_entities = parse_context_annotations(
                data)
            referenced_tweets = parse_referenced_tweets(data)
            hashtags = parse_hashtags(data)
            authors_args = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s)", x).decode(
                "utf-8") for x in [(conversation[1], None, None, None, None, None, None, None)])
            if authors_args:
                cur.execute("INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES " +
                            authors_args + " ON CONFLICT DO NOTHING;")
            parsed_conversations.append(conversation)
            parsed_annotations.append(annotations)
            parsed_links.append(links)
            parsed_context_annotations_domains.append(
                (context_annotations_domains, conversation[0]))
            parsed_context_annotations_entities.append(
                (context_annotations_entities, conversation[0]))
            parsed_referenced_tweets.append(referenced_tweets)
            for tag in hashtags:
                if tag not in hashtags_dict:
                    hashtags_dict[tag] = len(hashtags_dict) + 1
                    new_added_hashtags.append((hashtags_dict[tag], tag))
                new_added_hashtags_conversations.append(
                    (conversation[0], hashtags_dict[tag]))

            if len(parsed_conversations) >= 200000:
                insert_records(conn, cur, logger, parsed_conversations, parsed_annotations, parsed_links, parsed_referenced_tweets,
                               new_added_hashtags, new_added_hashtags_conversations, parsed_context_annotations_entities, parsed_context_annotations_domains)
                parsed_conversations = []
                parsed_annotations = []
                parsed_links = []
                parsed_context_annotations_domains = []
                parsed_context_annotations_entities = []
                parsed_referenced_tweets = []
                new_added_hashtags = []
                new_added_hashtags_conversations = []

        if len(parsed_conversations):
            insert_records(conn, cur, logger, parsed_conversations, parsed_annotations, parsed_links, parsed_referenced_tweets,
                           new_added_hashtags, new_added_hashtags_conversations, parsed_context_annotations_entities, parsed_context_annotations_domains)

    cur.close()
    conn.close()
