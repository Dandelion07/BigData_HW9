from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

def create_cassandra_keyspace(session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS books WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")

def create_cassandra_table_parents(session):
    create_table_query_parents = "CREATE TABLE books.parent3 (\
        title text,\
        url text,\
        contributors text,\
        date text,\
        format text,\
        trove_id text PRIMARY KEY,\
        pages int\
    );"
    session.execute(create_table_query_parents)

def insert_documents_to_cassandra(session, documents):
    insert_query_for_parents = "INSERT INTO books.parent3 (trove_id, title, url, contributors, date, format, pages) VALUES (?, ?, ?, ?, ?, ?, ?)"
    insert_statement = session.prepare(insert_query_for_parents)
    batch = BatchStatement()
    for doc in documents:
        row = (
            doc['_id'],
            doc['_source']['title'],
            doc['_source']['url'],
            doc['_source']['contributors'],
            doc['_source']['date'],
            doc['_source']['format'],
            int(doc['_source']['pages'])
        )
        batch.add(insert_statement, row)
    session.execute(batch)

def upload_documents(es, session):
    search_after = None
    i = 0
    while True:
        res = es.search(
            index='books',
            body={
                "query": {
                    "bool": {
                        "must_not": [
                            {
                                "term": {
                                    "parent": ""
                                }
                            }
                        ]
                    }
                },
                'sort': [{'_doc': 'asc'}],
                'size': 100,
                'search_after': search_after
            }
        )

        hits = res['hits']['hits']
        if not hits:
            break

        insert_documents_to_cassandra(session, hits)
        i += len(hits)
        search_after = hits[-1]['sort']
        print_progress(i)
    print_upload_complete(i)

def print_progress(count):
    print(f"{count} documents have been added to the parents table", end="\r")

def print_upload_complete(count):
    print(f"\n{count} documents have been uploaded\n")

def main():
    # Cassandra settings
    cluster = Cluster(['127.0.0.1'], port=32769)  # Replace with actual Cassandra cluster address
    session = cluster.connect()
    create_cassandra_keyspace(session)
    session.set_keyspace('books')
    create_cassandra_table_parents(session)

    # Elasticsearch settings
    es = Elasticsearch(['http://localhost:9200'])

    upload_documents(es, session)

if True:
    main()