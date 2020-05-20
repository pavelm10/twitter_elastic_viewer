import elasticsearch

from elasticsearch_reader import ElasticReader


if __name__ == "__main__":
    import argparse

    argp = argparse.ArgumentParser()

    argp.add_argument('--es-port',
                      dest='es_port',
                      help='elasticsearch server port',
                      default=9200)
    argp.add_argument('--es-host',
                      dest='es_host',
                      help='elasticsearch server host name',
                      default='localhost')
    argp.add_argument('--es-index',
                      dest='es_index',
                      help='elasticsearch index name to get data from',
                      default='tweets')
    argp.add_argument('--date',
                      help='start date to get data from',
                      required=True)

    args = argp.parse_args()

    es_reader = ElasticReader(args.es_host, args.es_port, args.es_index)
    try:
        ids = es_reader.read_all_ids(args.date)
        print("ES CONNECTION TEST: OK")

    except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as ex:
        print("ES CONNECTION TEST: NOK")
        raise ex
