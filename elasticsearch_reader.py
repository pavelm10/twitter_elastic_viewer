import logging
import elasticsearch
import elasticsearch.helpers


class ElasticReader:
    def __init__(self, es_host='localhost', es_port=9200, es_index=None):
        self.es_index = es_index
        self.elastic = elasticsearch.Elasticsearch(hosts=[{"host": es_host, "port": es_port}])
        self.log = logging.getLogger('root')

    def read_all_ids(self, start_date):
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "match_all": {}
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": start_date
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            }}

        try:
            query_gen = elasticsearch.helpers.scan(self.elastic, index=self.es_index, query=query, request_timeout=5)
            tweet_ids = {result['_source']['tweet_id'] for result in query_gen}
            return tweet_ids

        except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as ex:
            self.log.exception(ex)
            self.log.error("Connection to ES server failed!")
            return set()
